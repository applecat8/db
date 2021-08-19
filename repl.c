#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

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
    int file_descriptor;
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
    uint32_t num_rows;
    Pager *pager;
} Table;

// 一个Cursor对象，它代表了表中的一个位置
typedef struct {
    Table *table;
    uint32_t row_num;
    bool end_of_table; // 表示超过最后一个元素的一个位置
} Cursor;


InputBuffer* new_input_buffer();
void print_prompt();
void read_input(InputBuffer *input_buffer);
void close_input_buffer(InputBuffer *input_buffer);
MetaResult do_meta_command(InputBuffer *input_buffer, Table *table);
PreapareResult preapare_statement(InputBuffer *input_buffer, Statement *statement);
ExecuteResult execute_statement(Statement statement, Table *table);
void serialize_row(Row *source, void *destination);
void deserialize_row( void* source, Row *destination);
void* row_slot(Cursor *cursor);
void print_row(Row *row);
void free_table(Table *table);
PreapareResult preapare_insert(InputBuffer *input_buffer, Statement *statement);
Pager* pager_open(const char *filename);
void* get_page(Pager *pager, uint32_t page_num);
Table* db_open(const char *filename);
void db_close(Table *table);
void pager_flush(Pager *pager, uint32_t page_num, uint32_t size);
Cursor* table_start(Table *table);
Cursor* table_end(Table *table);
void cursor_advance(Cursor *cursor);

ExecuteResult execute_insert(Statement statement, Table *table);
ExecuteResult execute_select(Statement statement, Table *table);

const uint32_t ID_SIZE =size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username) - 1;
const uint32_t EMAIL_SIZE = size_of_attribute(Row,email) - 1;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;

const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROWS_PER_PAGE;

int main(int agc, char* argv[]){
    if (agc < 2) {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char *filename = argv[1];
    Table *table = db_open(filename);

    InputBuffer *input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer, table)) {
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
do_meta_command(InputBuffer *input_buffer, Table *table){
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        close_input_buffer(input_buffer);
        db_close(table);
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
    Cursor *cursor = table_end(table);
    serialize_row(row_to_insert, row_slot(cursor));
    
    table->num_rows++;
    free(cursor);
    return EXECUTE_SUCCESS;
}

// 使用游标来读取表，每循环一次游标推进1
ExecuteResult
execute_select(Statement statement, Table *table){
    Cursor* cursor = table_start(table);

    Row row;
    while (!(cursor->end_of_table)) {
        deserialize_row(row_slot(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }

    free(cursor);
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

// 获得 游标 在表中指向的地址
void*
row_slot(Cursor *cursor){
    uint32_t row_num = cursor->row_num;
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = get_page(cursor->table->pager, page_num);

    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

// 获取指定数值页的地址，如果该页不再内存中，则加载进内存
void*
get_page(Pager *pager, uint32_t page_num){
    if (page_num >= TABLE_MAX_PAGES ) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    // 缓存中没有, 分配内存，并将同一页的数据加载到内存
    if (pager->pages[page_num] == NULL) {
        void *page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE; // 得到现有完整的页数

        // 当存在不满一页的数据时，将其视为一页
        if (pager->file_length % PAGE_SIZE) {
            num_pages++;
        }

        // 如果访问的页中有数据
        if (page_num <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);

            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        pager->pages[page_num] = page;
    }
    return pager->pages[page_num];
}

// 新建表
Table*
db_open(const char *filename){
    Pager *pager = pager_open(filename);

    Table *table = malloc(sizeof(Table));
    table->pager = pager;
    uint32_t page_num = pager->file_length / PAGE_SIZE;
    uint32_t rows = (pager->file_length % PAGE_SIZE) / ROW_SIZE;
    table->num_rows = page_num * ROWS_PER_PAGE + rows;
    printf("%d", table->num_rows);
    return table;
}

// 关闭数据库
void
db_close(Table *table){
    Pager *pager = table->pager;
    uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

    // 处理填满的页面
    for (int i = 0; i < num_full_pages; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }
        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    // 处理剩下未满一页的数据
    uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
    if (num_additional_rows > 0) {
        uint32_t page_num = num_full_pages;
        if (pager->pages[num_full_pages] != NULL) {
            pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
            free(pager->pages[page_num]);
            pager->pages[page_num] = NULL;
        }
    }

    int result = close(pager->file_descriptor);
    if (result == -1) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void *page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }

    free(pager);
    free(table);
}

// 将表（内存中）中对应的页些入文件中
void 
pager_flush(Pager *pager, uint32_t page_num, uint32_t size){
    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }
    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);
    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

// 从内存中读取表文件
Pager*
pager_open(const char *filename){
    // 读写模式，不存在创建， 读写权限
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR );

    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager *pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }
    return pager;
}

Cursor*
table_start(Table *table){
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = 0;
    cursor->end_of_table= (table->num_rows == 0);

    return cursor;
}

Cursor*
table_end(Table *table){
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table= table;
    cursor->row_num = table->num_rows;
    cursor->end_of_table = true;

    return cursor;
}

// 推进游标
void
cursor_advance(Cursor *cursor){
    cursor->row_num++;
    if (cursor->row_num >= cursor->table->num_rows) {
        cursor->end_of_table = true;
    }
}
