use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub path: String,
    pub buffsize: u64,
    pub tables: Vec<MetaTableDef>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MetaType {
    LONG,
    DEC,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetaSchema {
    pub columns: Vec<(String, MetaType)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetaTableDef {
    pub name: String,
    pub file: String,
    pub filetype: String,
    pub schema: MetaSchema,
}

impl Metadata {
    pub fn from_default() -> Metadata {
        Metadata {
            path: "drivers/sample_queries/data/".to_string(),
            buffsize: 1048576,
            tables: vec![
                MetaTableDef {
                    name: "LINEITEM".to_string(),
                    file: "lineitem.tbl.bz2".to_string(),
                    filetype: "text".to_string(),
                    schema: MetaSchema {
                        columns: vec![
                            ("OKEY".to_string(), MetaType::LONG),
                            ("PKEY".to_string(), MetaType::LONG),
                            ("PRICE".to_string(), MetaType::DEC),
                        ],
                    },
                },
                MetaTableDef {
                    name: "ORDERS".to_string(),
                    file: "order.tbl.bz2".to_string(),
                    filetype: "text".to_string(),
                    schema: MetaSchema {
                        columns: vec![
                            ("OKEY".to_string(), MetaType::LONG),
                            ("ZIP".to_string(), MetaType::LONG),
                        ],
                    },
                },
                MetaTableDef {
                    name: "PART".to_string(),
                    file: "part.tbl".to_string(),
                    filetype: "text".to_string(),
                    schema: MetaSchema {
                        columns: vec![
                            ("PKEY".to_string(), MetaType::LONG),
                            ("COST".to_string(), MetaType::DEC),
                        ],
                    },
                },
            ],
        }
    }

    pub fn from_path(path: &str) -> Metadata {
        use std::fs::File;
        use std::io::Read;

        let mut file = File::open(path).unwrap();
        let mut data = String::new();
        file.read_to_string(&mut data).unwrap();

        serde_json::from_str(&data).expect("Failed to parse metadata file!")
    }
}
