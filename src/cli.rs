use clap::{Args, Parser};

#[derive(Parser, Debug)]
#[command(name = "spatialvault")]
#[command(author = "Jorge Martinez <jorge.martinezgomez@wfp.org>")]
#[command(version = "0.1")]
#[command(about = "Collect geospatial data from multiple sources", long_about = None)]
pub enum Cli {
    Msft(MsftArgs),
}

#[derive(Args, Debug)]
pub struct PgParams {
    #[arg(long = "database-url", group = "pg")]
    pub db_url: Option<String>,
    #[arg(requires="pg", long = "database-schema", default_value_t = String::from("public"))]
    pub db_schema: String,
    #[arg(requires="pg", long = "table-name", default_value_t = String::from("wld_buildings_microsoft"))]
    pub table_name: String,
    #[arg(requires = "pg", long = "create-table", default_value_t = false)]
    pub create_table: bool,
}

#[derive(Args, Debug)]
pub struct MsftArgs {
    #[arg(long, default_value_t = false)]
    pub list: bool,
    #[arg(long)]
    pub name: Option<String>,
    #[command(flatten)]
    pub pg_params: PgParams,
}
