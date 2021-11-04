//
// Created by Wind on 11/4/2021.
//
#include "db.h"


int main(int argc, const char *argv[]) {
    if (argc < 2) {
        printf("Must supply a database filename\n");
        return 0;
    }
    const char *filename = argv[1];
    Table *table = db_open(filename);
    Statement statement{};
    for (int i = 0; i < 15; ++i) {
        statement.row_to_insert.id = i + 1;
        sprintf(statement.row_to_insert.username, "user%d", i + 1);
        sprintf(statement.row_to_insert.email, "user%d@email.com", i + 1);
        statement.type = STATEMENT_INSERT;
        execute_statement(&statement, table);
    }
    statement.type = STATEMENT_SELECT;
    execute_statement(&statement, table);
    printf("Bye~\n");
    db_close(table);
    return 0;
}