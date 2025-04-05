use super::{OpIterator, TupleIterator};
use common::{AggOp, Attribute, CrustyError, DataType, Field, TableSchema, Tuple};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::env::current_dir;

/// Contains the index of the field to aggregate and the operator to apply to the column of each group. (You can add any other fields that you think are neccessary)
#[derive(Clone)]
pub struct AggregateField {
    /// Index of field being aggregated.
    pub field: usize,
    /// Agregate operation to aggregate the column with.
    pub op: AggOp,
}

/// Computes an aggregation function over multiple columns and grouped by multiple fields. (You can add any other fields that you think are neccessary)
struct Aggregator {
    /// Aggregated fields.
    agg_fields: Vec<AggregateField>,
    /// Group by fields
    groupby_fields: Vec<usize>,
    /// Schema of the output.
    schema: TableSchema,
    /// Hash map of group by fields to the corresponding aggregate fields.
    /// the first i32 is the count of the number of tuples in the group
    /// the second i32 is the type of field (0 for int, 1 for string)
    /// the third i32 is the sum of the field values (optional - only for avg and sum)
    groups: HashMap<Vec<Field>, Vec<(Field, i32, i32, Option<i32>)>>,
}

impl Aggregator {
    /// Aggregator constructor.
    ///
    /// # Arguments
    ///
    /// * `agg_fields` - List of `AggregateField`s to aggregate over. `AggregateField`s contains the aggregation function and the field to aggregate over.
    /// * `groupby_fields` - Indices of the fields to groupby over.
    /// * `schema` - TableSchema of the form [groupby_field attributes ..., agg_field attributes ...]).
    fn new(
        agg_fields: Vec<AggregateField>,
        groupby_fields: Vec<usize>,
        schema: &TableSchema,
    ) -> Self {
        Self {
            agg_fields,
            groupby_fields,
            schema: schema.clone(),
            groups: HashMap::new(),
        }
    }

    /// Handles the creation of groups for aggregation.
    ///
    /// If a group exists, then merge the tuple into the group's accumulated value.
    /// Otherwise, create a new group aggregate result.
    ///
    /// # Arguments
    ///
    /// * `tuple` - Tuple to add to a group.
    pub fn merge_tuple_into_group(&mut self, tuple: &Tuple) {
        // modify tuple to match the group by fields
        // get key for hashmap:
        let mut groupby_fields = Vec::new();
        for i in &self.groupby_fields {
            groupby_fields.push(tuple.get_field(*i).unwrap().clone());
        }
        // check if group exists
        if self.groups.contains_key(&groupby_fields) {
            // group exists
            let val = self.groups.get_mut(&groupby_fields).unwrap();
            let mut agg_fields = Vec::new();
            for (agg_idx, agg_field) in self.agg_fields.iter().enumerate() {
                let field_idx = agg_field.field;
                let curr = tuple.get_field(field_idx).unwrap().clone();
                // initialize new_field with null entries
                let mut new_field = (Field::Null, 0, 0, None);

                match agg_field.op {
                    AggOp::Avg => {
                        // take current average, count, and sum and use to calculate new average
                        let total = val[agg_idx].3.unwrap();
                        let sum = total + curr.unwrap_int_field();
                        let cnt = val[agg_idx].1 + 1;
                        // let new_cnt = cnt + 1;
                        let avg = (sum as f64 / cnt as f64).floor() as i32;
                        new_field.0 = Field::IntField(avg);
                        new_field.3 = Some(sum);
                    }
                    AggOp::Count => {
                        let cnt = val[agg_idx].1 + 1;
                        new_field.0 = Field::IntField(cnt);
                    }
                    AggOp::Max => {
                        match val[agg_idx].2 {
                            // check field type
                            0 => {
                                // int
                                let cur_max = val[agg_idx].0.unwrap_int_field();
                                let new_max = cur_max.max(curr.unwrap_int_field());
                                new_field.0 = Field::IntField(new_max);
                            }
                            1 => {
                                // string
                                let cur_max = val[agg_idx].0.unwrap_string_field();
                                let new_max = cur_max.max(curr.unwrap_string_field());
                                new_field.0 = Field::StringField(new_max.to_string());
                            }
                            2 => {
                                // null
                                new_field.0 = curr;
                            }
                            _ => panic!("Invalid field type"),
                        }
                    }
                    AggOp::Min => {
                        match val[agg_idx].2 {
                            0 => {
                                // int
                                let cur_min = val[agg_idx].0.unwrap_int_field();
                                let new_min = cur_min.min(curr.unwrap_int_field());
                                new_field.0 = Field::IntField(new_min);
                            }
                            1 => {
                                // string
                                let cur_min = val[agg_idx].0.unwrap_string_field();
                                let new_min = cur_min.min(curr.unwrap_string_field());
                                new_field.0 = Field::StringField(new_min.to_string());
                            }
                            2 => {
                                // null
                                new_field.0 = curr;
                            }
                            _ => panic!("Invalid field type"),
                        }
                    }
                    AggOp::Sum => match val[agg_idx].2 {
                        0 => {
                            let total = val[agg_idx].3.unwrap();
                            let sum = total + curr.unwrap_int_field();
                            new_field.0 = Field::IntField(sum);
                            new_field.3 = Some(sum);
                        }
                        _ => panic!("Sum only works on Int types"),
                    },
                };
                new_field.1 = val[agg_idx].1 + 1; // increment count
                                                  // keep other entries as is
                new_field.2 = val[agg_idx].2;
                if val[agg_idx].3.is_none() {
                    new_field.3 = None;
                }
                agg_fields.push(new_field);
            }
            // update hashmap
            self.groups.insert(groupby_fields, agg_fields);
        } else {
            // no matching group found
            let mut new_entry: Vec<(Field, i32, i32, Option<i32>)> = Vec::new();
            // add aggregate fields
            for agg_field in &self.agg_fields {
                let field_idx = agg_field.field;
                let curr = tuple.get_field(field_idx).unwrap().clone();
                let mut new_field = (Field::Null, 0, 0, None);

                new_field.0 = match agg_field.op {
                    AggOp::Avg => curr,
                    AggOp::Count => Field::IntField(1),
                    AggOp::Max => curr,
                    AggOp::Min => curr,
                    AggOp::Sum => curr,
                };
                new_field.1 = 1; // set count to 1

                let curr_borrow = tuple.get_field(field_idx).unwrap().clone();
                // set field type
                new_field.2 = match curr_borrow {
                    Field::IntField(_) => 0,
                    Field::StringField(_) => 1,
                    Field::Null => 2,
                };
                // set sum
                new_field.3 = match curr_borrow {
                    Field::IntField(num) => Some(num),
                    _ => None,
                };
                // add new field to entry
                new_entry.push(new_field);
            }
            // insert new entry into hashmap
            self.groups.insert(groupby_fields, new_entry);
        }
    }

    /// Returns a `TupleIterator` over the results.
    ///
    /// Resulting tuples must be of the form: (group by fields ..., aggregate fields ...)
    pub fn iterator(&self) -> TupleIterator {
        let mut tuples = Vec::new(); // vector of tuples
        for (groupby_fields, agg_fields) in &self.groups {
            let mut field_vec = Vec::new();
            // add groupby fields
            for field in groupby_fields {
                field_vec.push(field.clone());
            }
            // add aggregate fields
            for agg_field in agg_fields {
                field_vec.push(agg_field.0.clone());
            }
            // create new tuple and add to tuple vector
            let new_tuple = Tuple::new(field_vec);
            tuples.push(new_tuple);
        }
        let schema = self.schema.clone();
        TupleIterator::new(tuples, schema)
    }
}

/// Aggregate operator. (You can add any other fields that you think are neccessary)
pub struct Aggregate {
    /// Fields to groupby over.
    groupby_fields: Vec<usize>,
    /// Aggregation fields and corresponding aggregation functions.
    agg_fields: Vec<AggregateField>,
    /// Aggregation iterators for results.
    agg_iter: Option<TupleIterator>,
    /// Output schema of the form [groupby_field attributes ..., agg_field attributes ...]).
    schema: TableSchema,
    /// Boolean if the iterator is open.
    open: bool,
    /// Child operator to get the data from.
    child: Box<dyn OpIterator>,
}

impl Aggregate {
    /// Aggregate constructor.
    ///
    /// # Arguments
    ///
    /// * `groupby_indices` - the indices of the group by fields
    /// * `groupby_names` - the names of the group_by fields in the final aggregation
    /// * `agg_indices` - the indices of the aggregate fields
    /// * `agg_names` - the names of the aggreagte fields in the final aggregation
    /// * `ops` - Aggregate operations, 1:1 correspondence with the indices in agg_indices
    /// * `child` - child operator to get the input data from.
    pub fn new(
        groupby_indices: Vec<usize>,
        groupby_names: Vec<&str>,
        agg_indices: Vec<usize>,
        agg_names: Vec<&str>,
        ops: Vec<AggOp>,
        child: Box<dyn OpIterator>, // should be aggregator.iterator -> calculated tuples
    ) -> Self {
        // construct new schema by adding group by fields and aggregate fields
        let mut attributes = Vec::new();
        let schema = child.get_schema().clone();
        // add groupby fields
        for i in 0..groupby_indices.len() {
            let attr = schema.get_attribute(groupby_indices[i]).unwrap();
            let dtype = attr.dtype().clone();
            let name = groupby_names[i].to_string();
            attributes.push(Attribute::new(name, dtype));
        }
        // add aggregate fields
        let mut agg_fields = Vec::new();
        for i in 0..agg_indices.len() {
            // construct new aggregate field
            let agg_field = AggregateField {
                field: agg_indices[i],
                op: ops[i],
            };
            agg_fields.push(agg_field);

            // check if attribute is count: if so, make dtype an int
            let name = agg_names[i].to_string();
            match ops[i] {
                AggOp::Count => {
                    attributes.push(Attribute::new(name, DataType::Int));
                }
                _ => {
                    // get current dtype
                    let attr = schema.get_attribute(agg_indices[i]).unwrap();
                    let dtype = attr.dtype().clone();
                    attributes.push(Attribute::new(name, dtype));
                }
            }
        }
        Self {
            groupby_fields: groupby_indices,
            agg_fields,
            agg_iter: None,
            schema: TableSchema::new(attributes),
            open: false,
            child,
        }
    }
}

impl OpIterator for Aggregate {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;
        self.child.open()?;
        // create new aggregator
        let mut agg = Aggregator::new(
            self.agg_fields.clone(),
            self.groupby_fields.clone(),
            &self.schema,
        );
        // fill aggregator with tuples from child
        while let Ok(Some(tuple)) = self.child.next() {
            agg.merge_tuple_into_group(&tuple);
        }
        // initialize agg_iter
        self.agg_iter = Some(agg.iterator());
        self.agg_iter.as_mut().unwrap().open()?;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            return Err(CrustyError::CrustyError(
                "Operator has not been opened".to_string(),
            ));
        }
        // if there is an agg_iter then return next tuple
        if let Some(iter) = self.agg_iter.as_mut() {
            Ok(iter.next()?)
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            return Err(CrustyError::CrustyError(
                "Operator has already been closed".to_string(),
            ));
        }
        self.child.close()?;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            return Err(CrustyError::CrustyError(
                "Operator has not been opened".to_string(),
            ));
        }
        self.child.rewind()?;
        self.close()?;
        self.open()
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::opiterator::testutil::*;

    /// Creates a vector of tuples to create the following table:
    ///
    /// 1 1 3 E
    /// 2 1 3 G
    /// 3 1 4 A
    /// 4 2 4 G
    /// 5 2 5 G
    /// 6 2 5 G
    fn tuples() -> Vec<Tuple> {
        let tuples = vec![
            Tuple::new(vec![
                Field::IntField(1),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("E".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(2),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(3),
                Field::IntField(1),
                Field::IntField(4),
                Field::StringField("A".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(4),
                Field::IntField(2),
                Field::IntField(4),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(5),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(6),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
        ];
        tuples
    }

    mod aggregator {
        use super::*;
        use common::{DataType, Field};

        /// Set up testing aggregations without grouping.
        ///
        /// # Arguments
        ///
        /// * `op` - Aggregation Operation.
        /// * `field` - Field do aggregation operation over.
        /// * `expected` - The expected result.
        fn test_no_group(op: AggOp, field: usize, expected: i32) -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![Attribute::new("agg".to_string(), DataType::Int)]);
            let mut agg = Aggregator::new(vec![AggregateField { field, op }], Vec::new(), &schema);
            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(
                Field::IntField(expected),
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_merge_tuples_count() -> Result<(), CrustyError> {
            test_no_group(AggOp::Count, 0, 6)
        }

        #[test]
        fn test_merge_tuples_sum() -> Result<(), CrustyError> {
            test_no_group(AggOp::Sum, 1, 9)
        }

        #[test]
        fn test_merge_tuples_max() -> Result<(), CrustyError> {
            test_no_group(AggOp::Max, 0, 6)
        }

        #[test]
        fn test_merge_tuples_min() -> Result<(), CrustyError> {
            test_no_group(AggOp::Min, 0, 1)
        }

        #[test]
        fn test_merge_tuples_avg() -> Result<(), CrustyError> {
            test_no_group(AggOp::Avg, 0, 3)
        }

        #[test]
        #[should_panic]
        fn test_merge_tuples_not_int() {
            let _ = test_no_group(AggOp::Avg, 3, 3);
        }

        #[test]
        fn test_merge_multiple_ops() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("agg1".to_string(), DataType::Int),
                Attribute::new("agg2".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![
                    AggregateField {
                        field: 0,
                        op: AggOp::Max,
                    },
                    AggregateField {
                        field: 3,
                        op: AggOp::Count,
                    },
                ],
                Vec::new(),
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let expected = vec![Field::IntField(6), Field::IntField(6)];
            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(Tuple::new(expected), ai.next()?.unwrap());
            Ok(())
        }

        #[test]
        fn test_merge_tuples_one_group() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);
            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(3, rows);
            Ok(())
        }

        /// Returns the count of the number of tuples in an OpIterator.
        ///
        /// This function consumes the iterator.
        ///
        /// # Arguments
        ///
        /// * `iter` - Iterator to count.
        pub fn num_tuples(iter: &mut impl OpIterator) -> Result<u32, CrustyError> {
            let mut counter = 0;
            while iter.next()?.is_some() {
                counter += 1;
            }
            Ok(counter)
        }

        #[test]
        fn test_merge_tuples_multiple_groups() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group1".to_string(), DataType::Int),
                Attribute::new("group2".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![1, 2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(4, rows);
            Ok(())
        }
    }

    mod aggregate {
        use super::super::TupleIterator;
        use super::*;
        use common::{DataType, Field};

        fn tuple_iterator() -> TupleIterator {
            let names = vec!["1", "2", "3", "4"];
            let dtypes = vec![
                DataType::Int,
                DataType::Int,
                DataType::Int,
                DataType::String,
            ];
            let schema = TableSchema::from_vecs(names, dtypes);
            let tuples = tuples();
            TupleIterator::new(tuples, schema)
        }

        #[test]
        fn test_open() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            assert!(!ai.open);
            ai.open()?;
            assert!(ai.open);
            Ok(())
        }

        fn test_single_agg_no_group(
            op: AggOp,
            op_name: &str,
            col: usize,
            expected: Field,
        ) -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![col],
                vec![op_name],
                vec![op],
                Box::new(ti),
            );
            ai.open()?;
            assert_eq!(
                // Field::IntField(expected),
                expected,
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_single_agg() -> Result<(), CrustyError> {
            test_single_agg_no_group(AggOp::Count, "count", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Sum, "sum", 0, Field::IntField(21))?;
            test_single_agg_no_group(AggOp::Max, "max", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Min, "min", 0, Field::IntField(1))?;
            test_single_agg_no_group(AggOp::Avg, "avg", 0, Field::IntField(3))?;
            test_single_agg_no_group(AggOp::Count, "count", 3, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Max, "max", 3, Field::StringField("G".to_string()))?;
            test_single_agg_no_group(AggOp::Min, "min", 3, Field::StringField("A".to_string()))
        }

        #[test]
        fn test_multiple_aggs() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![3, 0, 0],
                vec!["count", "avg", "max"],
                vec![AggOp::Count, AggOp::Avg, AggOp::Max],
                Box::new(ti),
            );
            ai.open()?;
            let first_row: Vec<Field> = ai.next()?.unwrap().field_vals().cloned().collect();
            assert_eq!(
                vec![Field::IntField(6), Field::IntField(3), Field::IntField(6)],
                first_row
            );
            ai.close()
        }

        /// Consumes an OpIterator and returns a corresponding 2D Vec of fields
        pub fn iter_to_vec(iter: &mut impl OpIterator) -> Result<Vec<Vec<Field>>, CrustyError> {
            let mut rows = Vec::new();
            iter.open()?;
            while let Some(t) = iter.next()? {
                rows.push(t.field_vals().cloned().collect());
            }
            iter.close()?;
            Ok(rows)
        }

        #[test]
        fn test_multiple_aggs_groups() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![1, 2],
                vec!["group1", "group2"],
                vec![3, 0],
                vec!["count", "max"],
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            let mut result = iter_to_vec(&mut ai)?;
            result.sort();
            let expected = vec![
                vec![
                    Field::IntField(1),
                    Field::IntField(3),
                    Field::IntField(2),
                    Field::IntField(2),
                ],
                vec![
                    Field::IntField(1),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(3),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(4),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(5),
                    Field::IntField(2),
                    Field::IntField(6),
                ],
            ];
            assert_eq!(expected, result);
            ai.open()?;
            let num_rows = num_tuples(&mut ai)?;
            ai.close()?;
            assert_eq!(4, num_rows);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.next().unwrap();
        }

        #[test]
        fn test_close() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            assert!(ai.open);
            ai.close()?;
            assert!(!ai.open);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_close_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.close().unwrap();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.rewind().unwrap();
        }

        #[test]
        fn test_rewind() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![2],
                vec!["group"],
                vec![3],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            let count_before = num_tuples(&mut ai);
            ai.rewind()?;
            let count_after = num_tuples(&mut ai);
            ai.close()?;
            assert_eq!(count_before, count_after);
            Ok(())
        }

        #[test]
        fn test_get_schema() {
            let mut agg_names = vec!["count", "max"];
            let mut groupby_names = vec!["group1", "group2"];
            let ti = tuple_iterator();
            let ai = Aggregate::new(
                vec![1, 2],
                groupby_names.clone(),
                vec![3, 0],
                agg_names.clone(),
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            groupby_names.append(&mut agg_names);
            let expected_names = groupby_names;
            let schema = ai.get_schema();
            for (i, attr) in schema.attributes().enumerate() {
                // println!("i: {:?}", i);
                // println!("attr: {:?}", attr);
                assert_eq!(expected_names[i], attr.name());
                assert_eq!(DataType::Int, *attr.dtype());
            }
        }
    }
}
