#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100

// 元命令识别结果
typedef enum{
    META_SUCCESS,
    META_UNRECOGNIZED_COMMAND,
}MetaResult;

//语句类型
typedef enum {
    INSERT,
    SELECT,
}StatementType;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIVE_ID,
}PreapareResult;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FLL,
}ExecuteResult;

typedef struct {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
}InputBuffer;

typedef struct{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct{
    StatementType type;
    Row row_to_insert;
} Statement;

typedef struct {
    uint32_t num_rows;
    void *pages[TABLE_MAX_PAGES];
} Table;


InputBuffer* new_input_buffer();
void print_prompt();
void read_input(InputBuffer *input_buffer);
void close_input_buffer(InputBuffer *input_buffer);
MetaResult do_mate_command(InputBuffer *input_buffer);
PreapareResult preapare_statement(InputBuffer *input_buffer, Statement *statement);
ExecuteResult execute_statement(Statement statement, Table *table);
void serialize_row(Row *source, void *destination);
void deserialize_row( void* source, Row *destination);
void* row_slot(Table *table, uint32_t row_num);
void print_row(Row *row);
void free_table(Table *table);
PreapareResult preapare_insert(InputBuffer *input_buffer, Statement *statement);

ExecuteResult execute_insert(Statement statement, Table *table);
ExecuteResult execute_select(Statement statement, Table *table);
Table* new_table();

const uint32_t ID_SIZE =size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row,email);
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;

const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROWS_PER_PAGE;

int main(int agc, char* argv[]){
    Table* table = new_table();
    InputBuffer *input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (input_buffer->buffer[0] == '.') {
            switch (do_mate_command(input_buffer)) {
                case META_SUCCESS:
                    continue;
                case META_UNRECOGNIZED_COMMAND:
                    printf("Unrecognized command '%s'\n", input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (preapare_statement(input_buffer, &statement)) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_STRING_TOO_LONG:
                printf("String is too long.\n");
                continue;
            case PREPARE_NEGATIVE_ID:
                printf("ID must be positive.\n");
                continue;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized command '%s'\n", input_buffer->buffer);
                continue;
            case PREPARE_SYNTAX_ERROR:
                printf("syntax serror '%s' \n", input_buffer->buffer);
                continue;
        }

        switch (execute_statement(statement, table)) {
            case EXECUTE_SUCCESS:
                printf("Executed. \n");
                break;
            case EXECUTE_TABLE_FLL:
                printf("Error: tabe full.\n");
                break;
        }
    }
}

InputBuffer*
new_input_buffer(){
    InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

// 打印用户提示符
void
print_prompt(){
    printf("acdb >");
}

// 打印行
void
print_row(Row *row){
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

// 将读取的信息进行一个简单的封装
void
read_input(InputBuffer *input_buffer){
    //getline 的第一个参数会被分配, 所以关闭时要free
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }
    // 忽略结尾的换行符
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = '\0';
}

// 关闭input_buffer
void
close_input_buffer(InputBuffer *input_buffer){
    free(input_buffer->buffer);
    free(input_buffer);
}

// 识别原名令
MetaResult
do_mate_command(InputBuffer *input_buffer){
    if (!strcmp(input_buffer->buffer, ".exit")) {
        close_input_buffer(input_buffer);
        exit(EXIT_SUCCESS);
    }else {
        return META_UNRECOGNIZED_COMMAND;
    }
}

// 判读语句是否可以执行, 并将可执行的语句类型添加到信息中
PreapareResult
preapare_statement(InputBuffer *input_buffer, Statement *statement){
    if (!strncmp(input_buffer->buffer, "insert", 6)) {
        return preapare_insert(input_buffer, statement);
    }else if (!strcmp(input_buffer->buffer, "select")) {
        statement->type = SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

PreapareResult
preapare_insert(InputBuffer *input_buffer, Statement *statement){
    statement->type = INSERT;

    char *keyword = strtok(input_buffer->buffer, " ");
    char *id_str = strtok(NULL, " ");
    char *username= strtok(NULL, " ");
    char *email = strtok(NULL, " ");

    // 判断是否都存在
    if (!(id_str && username && email)) {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_str);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    // 长度是否满足
    if (strlen(username) > COLUMN_USERNAME_SIZE || strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);
    
    return PREPARE_SUCCESS;
}

ExecuteResult
execute_statement(Statement statement, Table *table){
    switch (statement.type) {
        case INSERT:
            return execute_insert(statement, table);
        case SELECT:
            return execute_select(statement, table);
    }
}

// 向表中插入数据
ExecuteResult
execute_insert(Statement statement, Table *table){
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FLL;
    }
    Row *row_to_insert = &(statement.row_to_insert);
    serialize_row(row_to_insert, row_slot(table, table->num_rows));
    
    table->num_rows++;
    return EXECUTE_SUCCESS;
}

ExecuteResult
execute_select(Statement statement, Table *table){
    Row row;
    for (uint32_t i = 0; i < table->num_rows; i++) {
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

// 将source信息写入表中的一行
void
serialize_row(Row *source, void *destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

// 从表中读取信息到 source中
// 此时 source是表的一行, destination存放读取的数据
void
deserialize_row(void *source, Row *destination){
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

// 获得 row_num 在表中对应的地址
void*
row_slot(Table *table, uint32_t row_num){
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = table->pages[page_num];
    if (page == NULL) {
        // 当我们初次访问一个未分配的也时，进行分配
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

// 新建表
Table*
new_table(){
    Table* table = malloc(sizeof(Table));
    table->num_rows = 0;
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        table->pages[i] = NULL;
    }
    return table;
}

// 释放表
void
free_table(Table *table){
    for (int i=0; table->pages[i]; i++) {
        free(table->pages[i]);
    }
    free(table);
}
