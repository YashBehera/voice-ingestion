package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"time"

	"voice-ingestion/internal/pipeline"
)

var (
	host     = flag.String("host", "127.0.0.1", "Worker UDP host")
	port     = flag.Int("port", 5004, "Worker UDP port")
	filePath = flag.String("file", "testdata/input.wav", "WAV file to stream")
	lossRate = flag.Int("loss", 0, "Percentage of packet loss to simulate (0-99)")
	useRed   = flag.Bool("red", true, "Enable RFC2198 RED packetization")
)

func main() {
	flag.Parse()

	log.Printf("Starting UDP Audio Sender...")
	log.Printf("Target: %s:%d", *host, *port)
	log.Printf("File: %s", *filePath)
	log.Printf("Loss Rate: %d%%", *lossRate)
	log.Printf("RFC2198 RED Enabled: %t", *useRed)

	// 1. Resolve target address
	raddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", *host, *port))
	if err != nil {
		log.Fatalf("Failed to resolve target address: %v", err)
	}

	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// 2. Open and parse input WAV file
	file, err := os.Open(*filePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// WAV parse using helper function in ingestion package
	// We scan the header and look for seekers to seek back to start of data
	// Wait, we need to read wav details
	fInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("Failed to stat file: %v", err)
	}
	log.Printf("WAV file size: %d bytes", fInfo.Size())

	// We can write a quick custom wav header parser or invoke standard read
	// Since we are running the sender, we can read chunks of WAV, resample to 48kHz,
	// encode to Opus, and send.
	// Let's implement a clean loop:
	// We read WAV file. Let's find sampleRate, channels.
	// We can use a local wav parser (just like in replay.go)
	
	// Seeking back is handled. Let's resolve the WAV headers
	// Note: We can reuse the parseWavHeader function if we export it, or rewrite it here.
	// Since it's in the ingestion package, but is private (parseWavHeader starts with a lowercase p),
	// we can write a quick inline parser or copy it. Copying it is very clean.
	sampleRate, channels, bitsPerSample, dataOffset, _, err := parseWavHeaderLocal(file)
	if err != nil {
		log.Fatalf("Failed to parse WAV header: %v", err)
	}
	log.Printf("Input Audio Format: %dHz, %d channels, %d-bit", sampleRate, channels, bitsPerSample)

	if bitsPerSample != 16 {
		log.Fatalf("Only 16-bit WAV is supported. Got %d-bit", bitsPerSample)
	}

	// Seek to data section
	if _, err := file.Seek(dataOffset, io.SeekStart); err != nil {
		log.Fatalf("Failed to seek to data section: %v", err)
	}

	// 3. Creates the Opus encoder object in memory and 
	// sets its target speed/quality to 24kbps (24,000 bits per second).
	encoder, err := pipeline.NewOpusEncoder(24000) // 24kbps
	if err != nil {
		log.Fatalf("Failed to create Opus encoder: %v", err)
	}

	// 4. Initialize RED Packer
	depth := 0
	if *useRed {
		depth = 2 // standard redundancy depth
	}
	packer := pipeline.NewRedPacker(depth)
	packer.Reset()

	// Spawn background listener to receive RTCP Receiver Reports and adjust RED depth dynamically
	go func() {
		rtcpBuf := make([]byte, 1024)
		for {
			n, err := conn.Read(rtcpBuf)
			if err != nil {
				return // socket closed
			}
			if n >= 21 {
				ssrc := binary.BigEndian.Uint32(rtcpBuf[0:4])
				fracLost := rtcpBuf[4]
				_ = binary.BigEndian.Uint32(rtcpBuf[5:9])    // TotalLost (unused locally but parsed)
				_ = binary.BigEndian.Uint32(rtcpBuf[9:13])   // HighestSeq (unused locally but parsed)
				jitter := binary.BigEndian.Uint32(rtcpBuf[13:17])
				recommended := int(binary.BigEndian.Uint32(rtcpBuf[17:21]))

				log.Printf("[RTCP Feedback] SSRC: %x | Loss Rate: %.1f%% | Jitter: %d | Recommended RED Depth: %d", 
					ssrc, float64(fracLost)/256.0*100.0, jitter, recommended)

				if *useRed {
					packer.SetDepth(recommended)
				}
			}
		}
	}()

	// We need to read 20ms chunks of WAV audio, resample to 48kHz, and encode
	// For 20ms:
	samplesPerChunk := (sampleRate * 20) / 1000
	bytesPerSample := 2
	bytesPerChunk := samplesPerChunk * channels * bytesPerSample

	wavBuf := make([]byte, bytesPerChunk)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	// Seed random number generator for packet loss simulation
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	totalSent := 0
	totalDropped := 0

	for range ticker.C {
		// Read one chunk
		n, err := io.ReadFull(file, wavBuf)
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					log.Printf("Finished streaming WAV file. Sent: %d, Dropped: %d (Loss: %.1f%%)", 
						totalSent, totalDropped, float64(totalDropped)/float64(totalSent+totalDropped)*100)
					return
				}
				log.Fatalf("Read error: %v", err)
			}

			// Convert to mono int16 PCM samples
			samplesReadCount := n / (channels * bytesPerSample)
			pcm := make([]int16, samplesReadCount)
			for i := 0; i < samplesReadCount; i++ {
				offset := i * channels * bytesPerSample
				if channels == 1 {
					pcm[i] = int16(binary.LittleEndian.Uint16(wavBuf[offset : offset+2]))
				} else {
					left := int16(binary.LittleEndian.Uint16(wavBuf[offset : offset+2]))
					right := int16(binary.LittleEndian.Uint16(wavBuf[offset+2 : offset+4]))
					pcm[i] = int16((int32(left) + int32(right)) / 2)
				}
			}

			// Resample to 48kHz (pipeline rate)
			resampled := pipeline.ResampleMonoPCM(pcm, sampleRate, pipeline.SampleRate)

			// The Opus encoder expects exactly 960 samples (20ms at 48kHz).
			// If our resampled chunk is not exactly 960 samples due to fractional ratios,
			// we pad or slice it. (Normally 16000 resampled to 48000 yields exactly 3x, so 320 -> 960).
			if len(resampled) < pipeline.FrameSamples {
				padding := make([]int16, pipeline.FrameSamples-len(resampled))
				resampled = append(resampled, padding...)
			} else if len(resampled) > pipeline.FrameSamples {
				resampled = resampled[:pipeline.FrameSamples]
			}

			// Encode PCM frame to Opus
			opusPayload, err := encoder.Encode(resampled)
			if err != nil {
				log.Printf("Opus encode error: %v", err)
				continue
			}

			// Pack into RED or Opus packet
			packet := packer.Pack(opusPayload)

			// Form RTP packet bytes
			// Payload type is 112 for RED, 111 for Opus
			pt := uint8(111)
			if *useRed {
				pt = 112
			}
			
			rtpData := makeRTPPacket(packet.SequenceNumber, packet.Timestamp, pt, packet.Payload)

			// 5. Simulate network packet loss
			if *lossRate > 0 && rng.Intn(100) < *lossRate {
				totalDropped++
				// Simulate loss by doing nothing (packet dropped)
				continue
			}

			// Send over UDP
			_, err = conn.Write(rtpData)
			if err != nil {
				log.Printf("UDP write error: %v", err)
				return
			}
			totalSent++
		}
}

// makeRTPPacket creates a standard 12-byte RTP header and appends the payload.
func makeRTPPacket(seq uint16, ts uint32, pt uint8, payload []byte) []byte {
	pkt := make([]byte, 12+len(payload))
	pkt[0] = 0x80 // Version: 2, CC: 0, Extension: 0, Padding: 0
	pkt[1] = pt & 0x7F
	binary.BigEndian.PutUint16(pkt[2:4], seq)
	binary.BigEndian.PutUint32(pkt[4:8], ts)
	binary.BigEndian.PutUint32(pkt[8:12], 98765432) // Fixed arbitrary SSRC
	copy(pkt[12:], payload)
	return pkt
}

func parseWavHeaderLocal(r io.Reader) (sampleRate, channels, bitsPerSample int, dataOffset, dataSize int64, err error) {
	header := make([]byte, 44)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, 0, 0, 0, 0, err
	}

	if string(header[0:4]) != "RIFF" || string(header[8:12]) != "WAVE" {
		return 0, 0, 0, 0, 0, fmt.Errorf("invalid WAV format")
	}

	channels = int(binary.LittleEndian.Uint16(header[22:24]))
	sampleRate = int(binary.LittleEndian.Uint32(header[24:28]))
	bitsPerSample = int(binary.LittleEndian.Uint16(header[34:36]))

	dataOffset = 44
	dataSize = int64(binary.LittleEndian.Uint32(header[40:44]))

	if string(header[36:40]) != "data" {
		// scan chunk offset
		if seeker, ok := r.(io.Seeker); ok {
			if _, err := seeker.Seek(12, io.SeekStart); err == nil {
				buf := make([]byte, 8)
				offset := int64(12)
				for {
					n, err := r.Read(buf)
					if err != nil || n < 8 {
						break
					}
					chunkID := string(buf[0:4])
					chunkSize := binary.LittleEndian.Uint32(buf[4:8])

					if chunkID == "fmt " {
						fmtBuf := make([]byte, 16)
						if _, err := io.ReadFull(r, fmtBuf); err == nil {
							channels = int(binary.LittleEndian.Uint16(fmtBuf[2:4]))
							sampleRate = int(binary.LittleEndian.Uint32(fmtBuf[4:8]))
							bitsPerSample = int(binary.LittleEndian.Uint16(fmtBuf[14:16]))
						}
						offset += 8 + int64(chunkSize)
						_, _ = seeker.Seek(offset, io.SeekStart)
					} else if chunkID == "data" {
						dataOffset = offset + 8
						dataSize = int64(chunkSize)
						break
					} else {
						offset += 8 + int64(chunkSize)
						_, _ = seeker.Seek(offset, io.SeekStart)
					}
				}
			}
		}
	}

	return sampleRate, channels, bitsPerSample, dataOffset, dataSize, nil
}
