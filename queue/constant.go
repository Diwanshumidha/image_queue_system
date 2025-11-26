package queue

// Define the topology constants as discussed in the design.
const (
	ExchangeName    = "image_events"
	ExchangeType    = "topic"
	MetadataQueue   = "q_metadata"
	TranscodeQueue  = "q_transcode"
	DeadLetterQueue = "q_failed_dlq"
	RoutingKey      = "upload.new" // Used by the API to publish new upload events
	BindingKey      = "upload.#"   // Binds all 'upload.' messages to the worker queues
)
