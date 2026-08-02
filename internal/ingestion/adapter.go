package ingestion

import "context"

// IngestionAdapter defines the interface for all media ingestion sources.
// This allows the system to easily support new ingestion protocols or sources
// by simply implementing this adapter interface.
type IngestionAdapter interface {
	// ID returns a unique identifier for this ingestion adapter instance.
	ID() string

	// Start begins listening to/reading from the source and pushing data to the pipeline.
	Start(ctx context.Context) error

	// Stop stops the ingestion process and cleans up any open network or file resources.
	Stop() error
}
