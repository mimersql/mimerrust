use mimerrust::{Connection, ToSql, CursorMode};

fn main() {
    print!("Connecting to database\n");
    let mut conn =
        Connection::open("", "RUSTUSER", "RUSTPASSWORD").unwrap_or_else(|ec| panic!("{}", ec));

    conn.execute_statement("DROP TABLE test_table").ok();
    println!("Creating table");
    conn.execute_statement("CREATE TABLE test_table (id INT primary key, text NVARCHAR(30))")
        .expect("Error creating table");
    println!("Inserting rows");
    let insert_stmt = conn.prepare("INSERT INTO test_table (id, text) VALUES(:id, :text)", 
        CursorMode::Forward).expect("Error preparing statement");

    let mut text = "Hello";
    let mut id = 1;
    let params: &[&dyn ToSql] = &[&id,&text];
    insert_stmt.execute_bind(params).expect("Error inserting first row"); 

    text = "World!";
    id = 2;
    let params: &[&dyn ToSql] = &[&id,&text];
    insert_stmt.execute_bind(params).expect("Error inserting second row");  

    let stmt = conn
        .prepare("SELECT * from test_table", CursorMode::Forward)
        .unwrap();
    let mut cursor = stmt.open_cursor().unwrap();
    println!("Fetching all rows");
    while let Some(row) = cursor.next_row().unwrap() {
        let id: i32 = row.get(1).unwrap().unwrap();
        let text: String = row.get(2).unwrap().unwrap();
        println!("id: {}, text: {}", id, text);
    }
}
