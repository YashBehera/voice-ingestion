// Web Audio API state
let audioContext = null;
let mediaStreamSource = null;
let processorNode = null;
let ws = null;

// UI elements
const btnMic = document.getElementById('btn-mic');
const btnReplay = document.getElementById('btn-replay');
const btnSlow = document.getElementById('btn-slow');
const btnCrash = document.getElementById('btn-crash');

const activeSourceBadge = document.getElementById('active-source-badge');
const workerStatus = document.getElementById('worker-status');

const metricSamples = document.getElementById('metric-samples');
const metricFrames = document.getElementById('metric-frames');
const latP50 = document.getElementById('lat-p50');
const latP95 = document.getElementById('lat-p95');
const latP99 = document.getElementById('lat-p99');

const netReceived = document.getElementById('net-received');
const netLost = document.getElementById('net-lost');
const netJitter = document.getElementById('net-jitter');
const netDropped = document.getElementById('net-dropped');

const transcriptsBox = document.getElementById('transcripts-box');
const vadLight = document.getElementById('vad-light');
const vadText = document.getElementById('vad-text');
const consumerListContainer = document.getElementById('consumer-list-container');

// Canvas visualizer
const canvas = document.getElementById('waveform-canvas');
const canvasCtx = canvas.getContext('2d');
let audioBufferForVisualization = new Float32Array(1024);

// Set canvas dimensions
function resizeCanvas() {
    canvas.width = canvas.parentElement.clientWidth;
    canvas.height = canvas.parentElement.clientHeight;
}
window.addEventListener('resize', resizeCanvas);
resizeCanvas();

// Render visualizer oscilloscope loop
function drawWaveform() {
    requestAnimationFrame(drawWaveform);
    
    const width = canvas.width;
    const height = canvas.height;
    
    canvasCtx.fillStyle = '#0a0b10';
    canvasCtx.fillRect(0, 0, width, height);
    
    // Draw grid lines
    canvasCtx.strokeStyle = 'rgba(255, 255, 255, 0.03)';
    canvasCtx.lineWidth = 1;
    canvasCtx.beginPath();
    canvasCtx.moveTo(0, height / 2);
    canvasCtx.lineTo(width, height / 2);
    canvasCtx.stroke();
    
    // Draw audio wave
    canvasCtx.strokeStyle = ws && ws.readyState === WebSocket.OPEN ? '#10b981' : '#4b5563';
    canvasCtx.lineWidth = 2.5;
    canvasCtx.shadowBlur = 8;
    canvasCtx.shadowColor = ws && ws.readyState === WebSocket.OPEN ? '#10b981' : 'transparent';
    canvasCtx.beginPath();
    
    const sliceWidth = width / audioBufferForVisualization.length;
    let x = 0;
    
    for (let i = 0; i < audioBufferForVisualization.length; i++) {
        // scale visual representation
        const v = audioBufferForVisualization[i] * 2.5; 
        const y = (height / 2) + v * (height / 2);
        
        if (i === 0) {
            canvasCtx.moveTo(x, y);
        } else {
            canvasCtx.lineTo(x, y);
        }
        
        x += sliceWidth;
    }
    
    canvasCtx.lineTo(width, height / 2);
    canvasCtx.stroke();
    canvasCtx.shadowBlur = 0; // reset shadow
}
drawWaveform();

// Toggle browser microphone capture
btnMic.addEventListener('click', async () => {
    if (ws && ws.readyState === WebSocket.OPEN) {
        stopMicrophone();
    } else {
        await startMicrophone();
    }
});

async function startMicrophone() {
    try {
        btnMic.disabled = true;
        btnMic.innerText = "Connecting...";

        const stream = await navigator.mediaDevices.getUserMedia({
            audio: {
                echoCancellation: true,
                noiseSuppression: true,
                autoGainControl: true
            },
            video: false
        });

        // 1. Establish WebSocket connection targeting 16000Hz PCM
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws/ingest?rate=16000`;
        ws = new WebSocket(wsUrl);
        ws.binaryType = 'arraybuffer';

        ws.onopen = () => {
            logStatus("WebSocket Connected");
            btnMic.classList.add('active');
            btnMic.disabled = false;
            btnMic.innerHTML = `
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><line x1="9" y1="9" x2="15" y2="15"/><line x1="15" y1="9" x2="9" y2="15"/></svg>
                Stop Mic
            `;

            // 2. Setup Web Audio context resampled to 16kHz
            audioContext = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 16000 });
            mediaStreamSource = audioContext.createMediaStreamSource(stream);

            // ScriptProcessorNode size 2048, 1 input channel, 1 output channel
            processorNode = audioContext.createScriptProcessor(2048, 1, 1);
            
            processorNode.onaudioprocess = (e) => {
                const inputData = e.inputBuffer.getChannelData(0);
                
                // Copy for local visualization canvas
                audioBufferForVisualization.set(inputData.slice(0, 1024));

                if (ws && ws.readyState === WebSocket.OPEN) {
                    // Convert Float32 (-1.0 to 1.0) to Int16 (-32768 to 32767)
                    const pcmData = new Int16Array(inputData.length);
                    for (let i = 0; i < inputData.length; i++) {
                        let sample = inputData[i] * 32767;
                        if (sample > 32767) sample = 32767;
                        if (sample < -32768) sample = -32768;
                        pcmData[i] = sample;
                    }
                    // Send binary Int16 data over websocket
                    ws.send(pcmData.buffer);
                }
            };

            mediaStreamSource.connect(processorNode);
            processorNode.connect(audioContext.destination);
        };

        ws.onclose = () => {
            stopMicrophone();
        };

        ws.onerror = (err) => {
            console.error("WebSocket error:", err);
            stopMicrophone();
        };

    } catch (err) {
        console.error("Mic access failed:", err);
        stopMicrophone();
    }
}

function stopMicrophone() {
    btnMic.disabled = false;
    btnMic.classList.remove('active');
    btnMic.innerHTML = `
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2a3 3 0 0 0-3 3v7a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3Z"/><path d="M19 10v1a7 7 0 0 1-14 0v-1"/><line x1="12" x2="12" y1="19" y2="22"/></svg>
        Browser Mic
    `;

    if (processorNode) {
        processorNode.disconnect();
        processorNode = null;
    }
    if (mediaStreamSource) {
        mediaStreamSource.disconnect();
        mediaStreamSource = null;
    }
    if (audioContext) {
        audioContext.close();
        audioContext = null;
    }
    if (ws) {
        ws.close();
        ws = null;
    }

    audioBufferForVisualization.fill(0);
    logStatus("Microphone Stopped");
}

function logStatus(msg) {
    workerStatus.innerText = msg;
}

// Replay controls
let replaying = false;
btnReplay.addEventListener('click', async () => {
    try {
        if (replaying) {
            const res = await fetch('/api/replay/stop', { method: 'POST' });
            if (res.ok) {
                replaying = false;
                btnReplay.classList.remove('active');
                btnReplay.innerHTML = `
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="5 3 19 12 5 21 5 3"/></svg>
                    Start File Replay
                `;
            }
        } else {
            const res = await fetch('/api/replay/start', { method: 'POST' });
            if (res.ok) {
                replaying = true;
                btnReplay.classList.add('active');
                btnReplay.innerHTML = `
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="4" width="16" height="16" rx="2" ry="2"/></svg>
                    Stop Replay
                `;
            }
        }
    } catch (err) {
        console.error("Replay api call failed:", err);
    }
});

// Slow Consumer simulations
let slowActive = false;
btnSlow.addEventListener('click', async () => {
    try {
        const endpoint = slowActive ? '/api/consumer/slow/stop' : '/api/consumer/slow/start';
        const res = await fetch(endpoint, { method: 'POST' });
        if (res.ok) {
            slowActive = !slowActive;
            btnSlow.classList.toggle('active', slowActive);
            btnSlow.innerText = slowActive ? "Deregister Slow" : "Simulate Slow";
        }
    } catch (err) {
        console.error("Slow consumer toggle failed:", err);
    }
});

// Crash simulation
btnCrash.addEventListener('click', async () => {
    try {
        const res = await fetch('/api/consumer/crash', { method: 'POST' });
        if (res.ok) {
            logStatus("Simulated Panic Triggered!");
            setTimeout(() => logStatus("Worker Recovered"), 2000);
        }
    } catch (err) {
        console.error("Crash trigger failed:", err);
    }
});

// Poll server metrics
async function pollStats() {
    try {
        const res = await fetch('/api/status');
        if (!res.ok) return;
        const data = await res.json();

        // Update Ingestion stats
        metricSamples.innerText = data.pipeline.samples_ingested.toLocaleString();
        metricFrames.innerText = data.pipeline.frames_encoded.toLocaleString();
        
        activeSourceBadge.innerText = data.pipeline.active_source || "No Active Source";
        if (data.pipeline.active_source) {
            logStatus("Ingesting Stream...");
        } else if (!ws && !replaying) {
            logStatus("Pipeline Idle");
        }

        // Update latency percentiles
        latP50.innerText = data.analytics.p50_latency_ms.toFixed(2);
        latP95.innerText = data.analytics.p95_latency_ms.toFixed(2);
        latP99.innerText = data.analytics.p99_latency_ms.toFixed(2);

        // Update Network metrics
        netReceived.innerText = data.analytics.packets_received;
        netLost.innerText = data.analytics.missing_count;
        netJitter.innerText = `${data.analytics.jitter_ms.toFixed(2)} ms`;

        netDropped.innerText = data.analytics.recovered_count;

        // Update VAD transcription panel
        if (data.speech.speech_detected) {
            vadLight.classList.add('active');
            vadText.innerText = `Speaking (RMS: ${data.speech.current_rms.toFixed(3)})`;
        } else {
            vadLight.classList.remove('active');
            vadText.innerText = "Silence";
        }

        // Render transcripts
        if (data.speech.transcripts && data.speech.transcripts.length > 0) {
            transcriptsBox.innerHTML = '';
            data.speech.transcripts.forEach(line => {
                const div = document.createElement('div');
                div.className = 'transcript-line';
                div.innerText = line;
                transcriptsBox.appendChild(div);
            });
            // scroll to bottom
            transcriptsBox.scrollTop = transcriptsBox.scrollHeight;
        } else {
            if (transcriptsBox.innerHTML.trim() === '' || transcriptsBox.children.length === 0) {
                transcriptsBox.innerHTML = '<div style="color: var(--text-secondary); text-align: center; margin: auto;">Speak to transcribe or start replay...</div>';
            }
        }

        // Render Consumers list
        if (data.consumers) {
            consumerListContainer.innerHTML = '';
            data.consumers.forEach(c => {
                const item = document.createElement('div');
                item.className = 'consumer-item';
                
                let dotClass = 'active';
                if (c.ID === 'slow-consumer') {
                    dotClass = 'warn';
                }
                if (c.ID === 'crashy-consumer') {
                    // Check if crashed (if we triggered crash and it's not active anymore)
                    dotClass = 'active';
                }

                item.innerHTML = `
                    <div class="consumer-info">
                        <div class="consumer-name">${c.ID}</div>
                        <div class="consumer-details">Queue: ${c.Len} / ${c.Cap} | Dropped: ${c.Dropped}</div>
                    </div>
                    <div class="consumer-status"><div class="status-dot ${dotClass}"></div></div>
                `;
                consumerListContainer.appendChild(item);
            });
        }

    } catch (err) {
        console.error("Error polling stats:", err);
    }
}

// Start polling
setInterval(pollStats, 500);
pollStats();
