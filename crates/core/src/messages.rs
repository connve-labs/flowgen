use arrow::array::RecordBatch;

#[derive(Debug, Clone)]
pub struct FileMessage {
    pub record_batch: RecordBatch,
    pub file_chunk: String,
}

#[derive(Debug, Clone)]
#[allow(non_camel_case_types)]
pub enum ChannelMessage {
    FileMessage(FileMessage),
}
