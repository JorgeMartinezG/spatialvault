use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "spatialvault")]
#[command(author = "Jorge Martinez <jorge.martinezgomez@wfp.org>")]
#[command(version = "0.1")]
#[command(about = "Collect geospatial data from multiple sources", long_about = None)]
pub enum Cli {
    Msft(MsftArgs),
}

#[derive(clap::Args, Debug)]
pub struct MsftArgs {
    #[arg(long, default_value_t = false)]
    pub list: bool,
    #[arg(long = "create-table", default_value_t = false)]
    pub create_table: bool,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long = "database-url")]
    pub db_url: Option<String>,
    #[arg(long = "database-schema", default_value_t = String::from("public"))]
    pub db_schema: String,
    #[arg(long = "table-name", default_value_t = String::from("wld_buildings_microsoft"))]
    pub table_name: String,
}
