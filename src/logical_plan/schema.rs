// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/*
 * @Author: Veeupup
 * @Date: 2022-05-18 13:45:10
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-18 14:54:46
 *
 * Arrow Field does not have table/relation name as its proroties
 * So we need a Schema to define inner schema with table name
 *
 * Code Ideas come from https://github.com/apache/arrow-datafusion/
 *
 */

use std::ptr::null;

use arrow::datatypes::{DataType, SchemaRef};
use arrow::datatypes::{Field, Schema};

use crate::error::ErrorCode;
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct NaiveSchema {
    pub fields: Vec<NaiveField>,
}

impl NaiveSchema {
    pub fn empty() -> Self {
        Self { fields: vec![] }
    }

    pub fn new(fields: Vec<NaiveField>) -> Self {
        // TODO(veeupup): check if we have duplicated name field
        Self { fields }
    }

    pub fn from_qualified(qualifier: &str, schema: &Schema) -> Self {
        Self::new(
            schema
                .fields()
                .iter()
                .map(|field| NaiveField {
                    field: field.clone(),
                    qualifier: Some(qualifier.to_owned()),
                })
                .collect(),
        )
    }

    pub fn from_unqualified(schema: &Schema) -> Self {
        Self::new(
            schema
                .fields()
                .iter()
                .map(|field| NaiveField {
                    field: field.clone(),
                    qualifier: None,
                })
                .collect(),
        )
    }

    /// join two schema
    pub fn join(&self, schema: &NaiveSchema) -> Self {
        let mut fields = self.fields.clone();
        fields.extend_from_slice(schema.fields().as_slice());
        Self::new(fields)
    }

    pub fn fields(&self) -> &Vec<NaiveField> {
        &self.fields
    }

    pub fn field(&self, i: usize) -> &NaiveField {
        &self.fields[i]
    }

    pub fn index_of(&self, name: &str) -> Result<usize> {
        for i in 0..self.fields().len() {
            if self.fields[i].name() == name {
                return Ok(i);
            }
        }
        Err(ErrorCode::NoSuchField)
    }

    /// Find the field with the given name
    pub fn field_with_name(&self, relation_name: Option<&str>, name: &str) -> Result<NaiveField> {
        if let Some(relation_name) = relation_name {
            self.field_with_qualified_name(relation_name, name)
        } else {
            self.field_with_unqualified_name(name)
        }
    }

    pub fn field_with_unqualified_name(&self, name: &str) -> Result<NaiveField> {
        let matches = self
            .fields
            .iter()
            .filter(|field| field.name() == name)
            .collect::<Vec<_>>();
        match matches.len() {
            0 => Err(ErrorCode::PlanError(format!("No field named '{}'", name))),
            1 => Ok(matches[0].to_owned()),
            _ => Err(ErrorCode::PlanError(format!(
                "Ambiguous reference to field named '{}'",
                name
            ))),
        }
    }

    pub fn field_with_qualified_name(&self, relation_name: &str, name: &str) -> Result<NaiveField> {
        let matches = self
            .fields
            .iter()
            .filter(|field| {
                field.qualifier == Some(relation_name.to_owned()) && field.name() == name
            })
            .collect::<Vec<_>>();
        match matches.len() {
            0 => Err(ErrorCode::PlanError(format!("No field named '{}'", name))),
            1 => Ok(matches[0].to_owned()),
            _ => Err(ErrorCode::PlanError(format!(
                "Ambiguous reference to field named '{}'",
                name
            ))),
        }
    }
}

impl Into<Schema> for NaiveSchema {
    /// Convert a schema into a DFSchema
    fn into(self) -> Schema {
        Schema::new(
            self.fields
                .into_iter()
                .map(|f| {
                    if f.qualifier().is_some() {
                        Field::new(
                            f.qualified_name().as_str(),
                            f.data_type().to_owned(),
                            f.is_nullable(),
                        )
                    } else {
                        f.field
                    }
                })
                .collect(),
        )
    }
}

impl Into<SchemaRef> for NaiveSchema {
    fn into(self) -> SchemaRef {
        SchemaRef::new(self.into())
    }
}

/// NaiveField wraps an Arrow field and adds an optional qualifier
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NaiveField {
    /// Optional qualifier (usually a table or relation name)
    qualifier: Option<String>,
    /// Arrow field definition
    field: Field,
}

impl NaiveField {
    pub fn new(qualifier: Option<&str>, name: &str, data_type: DataType, nullable: bool) -> Self {
        Self {
            qualifier: qualifier.map(|s| s.to_owned()),
            field: Field::new(name, data_type, nullable),
        }
    }

    pub fn from(field: Field) -> Self {
        Self {
            qualifier: None,
            field,
        }
    }

    pub fn from_qualified(qualifier: &str, field: Field) -> Self {
        Self {
            qualifier: Some(qualifier.to_owned()),
            field,
        }
    }

    pub fn name(&self) -> &String {
        self.field.name()
    }

    /// Returns an immutable reference to the `NaiveField`'s data-type
    pub fn data_type(&self) -> &DataType {
        &self.field.data_type()
    }

    /// Indicates whether this `NaiveField` supports null values
    pub fn is_nullable(&self) -> bool {
        self.field.is_nullable()
    }

    /// Returns a reference to the `NaiveField`'s qualified name
    pub fn qualified_name(&self) -> String {
        if let Some(relation_name) = &self.qualifier {
            format!("{}.{}", relation_name, self.field.name())
        } else {
            self.field.name().to_owned()
        }
    }

    /// Get the optional qualifier
    pub fn qualifier(&self) -> Option<&String> {
        self.qualifier.as_ref()
    }
}
