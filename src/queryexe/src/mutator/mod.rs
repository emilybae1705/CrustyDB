use crate::StorageManager;
use common::{prelude::*, storage_trait::StorageTrait, ConversionError, ConvertedResult};
use sqlparser::ast::{Value, Values};
use std::{fmt::Display, fs, path::Path};

pub(crate) fn insert_validated_tuples(
    table_id: ContainerId,
    tuples: Vec<Tuple>,
    txn_id: TransactionId,
    sm: &'static StorageManager,
) -> Result<usize, CrustyError> {
    let mut tuples_bytes = Vec::new();
    warn!("Not using TM or indexes with inserting new tuples");
    for t in &tuples {
        tuples_bytes.push(t.to_bytes());
    }
    let inserted = sm.insert_values(table_id, tuples_bytes, txn_id);
    let insert_count = inserted.len();
    if insert_count == tuples.len() {
        Ok(insert_count)
    } else {
        Err(CrustyError::ExecutionError(format!(
            "Attempting to insert {} tuples and only {} were inserted",
            tuples.len(),
            insert_count
        )))
    }
}

/// Check new or updated records to ensure that they do not break any constraints
pub(crate) fn validate_tuples(
    _table_id: &ContainerId,
    schema: &TableSchema,
    col_order: Option<Vec<usize>>,
    mut values: ConvertedResult,
    _txn_id: &TransactionId,
) -> Result<ConvertedResult, CrustyError> {
    if col_order.is_some() {
        return Err(CrustyError::CrustyError(String::from(
            "Col ordering not supported",
        )));
    }
    let mut values_to_remove: Vec<(usize, Vec<ConversionError>)> = Vec::new();
    warn!("PK, FK, Unique constaints not checked");
    for (i, rec) in values.converted.iter().enumerate() {
        for (j, (field, attr)) in (rec.field_vals()).zip(schema.attributes()).enumerate() {
            if let Field::Null = field {
                match attr.constraint {
                    common::Constraint::NotNull
                    | common::Constraint::UniqueNotNull
                    | common::Constraint::PrimaryKey
                    | common::Constraint::NotNullFKey(_) => {
                        values_to_remove.push((i, vec![ConversionError::NullFieldNotAllowed(j)]));
                    }
                    _ => continue, // Null value so nothing to check
                }
            }
            match &attr.dtype {
                DataType::Int => {
                    if let Field::IntField(_v) = field {
                        // Nothing for now
                    } else {
                        values_to_remove.push((i, vec![ConversionError::WrongType]));
                    }
                }
                DataType::String => {
                    if let Field::StringField(_v) = field {
                        // Nothing for now
                    } else {
                        values_to_remove.push((i, vec![ConversionError::WrongType]));
                    }
                }
            }
        }
    }
    // Remove in reverse order records that were invalid
    for i in values_to_remove.iter().rev() {
        values.converted.remove(i.0);
    }
    Ok(values)
}

/// convert data from CSV into internal representation
pub(crate) fn convert_csv_data<P: AsRef<Path> + Display>(
    path: P,
) -> Result<ConvertedResult, CrustyError> {
    let mut res = ConvertedResult {
        converted: Vec::new(),
        unconverted: Vec::new(),
    };
    // Convert path into an absolute path.
    let path = fs::canonicalize(path)?;
    debug!("server::csv_utils trying to open file, path: {:?}", path);
    let file = fs::File::open(path)?;
    // Create csv reader.
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(file);

    // Iterate through csv records.
    let mut inserted_records = 0;
    for result in rdr.records() {
        #[allow(clippy::single_match)]
        match result {
            Ok(rec) => {
                // Build tuple and infer types from schema.
                let mut tuple = Tuple::new(Vec::new());
                for field in rec.iter() {
                    if field.eq("null") {
                        tuple.field_vals.push(Field::Null);
                    } else {
                        let value = field.parse::<i32>();
                        match value {
                            Ok(num) => tuple.field_vals.push(Field::IntField(num)),
                            Err(_) => tuple.field_vals.push(Field::StringField(field.to_owned())),
                        }
                    }
                }
                inserted_records += 1;
                res.converted.push(tuple);
            }
            _ => {
                // FIXME: get error from csv reader
                error!("Could not read row from CSV");
                return Err(CrustyError::IOError(
                    "Could not read row from CSV".to_string(),
                ));
            }
        }
    }
    info!("Num records imported: {:?}", inserted_records);
    Ok(res)
}

/// Convert data from SQL parser insert and convert to internal representation
pub(crate) fn convert_insert_vals(values: &Values) -> Result<ConvertedResult, CrustyError> {
    let mut res = ConvertedResult {
        converted: Vec::new(),
        unconverted: Vec::new(),
    };
    for (i, val) in values.0.iter().enumerate() {
        let mut fields = Vec::new();
        for field in val {
            if let sqlparser::ast::Expr::Value(value) = field {
                match value {
                    Value::Number(val, is_long) => {
                        if *is_long {
                            res.unconverted
                                .push((i, vec![ConversionError::UnsupportedType]))
                        } else {
                            let f = val.parse::<i32>();
                            match f {
                                Ok(converted_field) => {
                                    fields.push(Field::IntField(converted_field))
                                }
                                Err(_) => {
                                    res.unconverted.push((i, vec![ConversionError::ParseError]))
                                }
                            };
                        }
                    }
                    Value::DoubleQuotedString(val) | Value::SingleQuotedString(val) => {
                        fields.push(Field::StringField(val.to_string()));
                    }
                    Value::Null => {
                        fields.push(Field::Null);
                    }
                    _ => {
                        res.unconverted
                            .push((i, vec![ConversionError::UnsupportedType]));
                    }
                }
            }
        }
        res.converted.push(Tuple::new(fields));
    }
    Ok(res)
}
