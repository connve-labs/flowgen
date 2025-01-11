use arrow::{
    array::{Array, RecordBatch, StringArray},
    datatypes::{DataType, Field},
};
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with an Apache Arrow data.")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("Provided value is not an object.")]
    NotObject(),
}
pub trait RecordBatchExt {
    type Error;
    fn to_recordbatch(&self) -> Result<arrow::array::RecordBatch, Self::Error>;
}

impl RecordBatchExt for serde_json::Value {
    type Error = Error;
    fn to_recordbatch(&self) -> Result<arrow::array::RecordBatch, Self::Error> {
        let map = self.as_object().ok_or_else(Error::NotObject)?;
        let mut fields = Vec::new();
        let mut values = Vec::new();

        for (key, value) in map {
            fields.push(Field::new(key, DataType::Utf8, true));
            let array = StringArray::from(vec![Some(value.to_string())]);
            values.push(Arc::new(array));
        }

        let columns = values
            .into_iter()
            .map(|x| x as Arc<dyn Array>)
            .collect::<Vec<Arc<dyn Array>>>();

        let schema = arrow::datatypes::Schema::new(fields);
        let batch = RecordBatch::try_new(Arc::new(schema), columns).map_err(Error::Arrow)?;
        Ok(batch)
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub data: arrow::array::RecordBatch,
    pub subject: String,
    pub current_task_index: Option<usize>,
}
