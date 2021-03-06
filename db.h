#ifndef _REPL_H
#define _REPL_H

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
    EXECUTE_DUPLICATE_KEY,
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

// 语句
typedef struct{
    StatementType type; // 语句类型
    Row row_to_insert; // 插入语句
} Statement;

typedef struct {
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
    uint32_t root_page_num;
    Pager *pager;
} Table;
    
// 节点类型
typedef enum {
    NODE_INTERNAL,
    NODE_LEAF,
}NodeType;

// 现在它是一棵树，我们通过节点的页码和该节点中的单元格编号来确定一个位置。
typedef struct {
    Table *table;
    uint32_t page_num;
    uint32_t cell_num;
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
void print_row(Row *row);
void print_tree(Pager *pager, uint32_t page_num, uint32_t indentation_level);
void free_table(Table *table);
PreapareResult preapare_insert(InputBuffer *input_buffer, Statement *statement);
Pager* pager_open(const char *filename);
void* get_page(Pager *pager, uint32_t page_num);
Table* db_open(const char *filename);
void db_close(Table *table);
void pager_flush(Pager *pager, uint32_t page_num);
Cursor* table_start(Table *table);
Cursor* table_find(Table *table, uint32_t key);
void cursor_advance(Cursor *cursor);
void* cursor_value(Cursor *cursor);
ExecuteResult execute_insert(Statement statement, Table *table);
ExecuteResult execute_select(Statement statement, Table *table);
void leaf_node_split_and_insert(Cursor *cursor, uint32_t key, Row *value);
uint32_t get_unused_page_num(Pager *pager);
void create_new_root(Table *table, uint32_t pright_child_page_num);

// 获得给定节点的类型
NodeType get_node_type(void *node);
// 设置给定节点的类型
void set_node_type(void *node, NodeType type);
// 根据叶节点的首地址，得到该节点的 num_cells 信息
uint32_t* leaf_node_num_cells(void *node);
// 得到 node节点的第 cell_num 个cell(键值信息) 的地址
void* leaf_node_cell(void *node, uint32_t cell_num);
// 得到 node节点的第 cell_num 个key 的地址
uint32_t* leaf_node_key(void *node, uint32_t cell_num);
// 得到 node节点的第 cell_num 个value 的地址
uint32_t* leaf_node_value(void *node, uint32_t cell_num);
// 初始化一个节点，即将该节点的num_cells值置为0
void initialize_leaf_node(void *node);
// 在当前游标下插入一条数据
void leaf_node_insert(Cursor *cursor, uint32_t key, Row *value);
Cursor *leaf_node_find(Table *table, uint32_t page_num, uint32_t key);
Cursor *internal_node_find(Table *table, uint32_t page_num, uint32_t key);

// 打印当前的常量
void print_constants();
void indent(uint32_t level);
bool is_node_root(void *node);
void set_node_root(void *node, bool is_root);
uint32_t get_node_max_key(void *node);

uint32_t *internal_node_num_keys(void *node);
uint32_t *internal_node_right_child(void *node);
uint32_t *internal_node_cell(void *node, uint32_t cell_num);
uint32_t *internal_node_child(void *node, uint32_t child_num);
uint32_t *internal_node_key(void *node, uint32_t key_num);

const uint32_t PAGE_SIZE = 4096;
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username) - 1;
const uint32_t EMAIL_SIZE = size_of_attribute(Row,email) - 1;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;

/*
 * 公共节点头布局
 */
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_OFFSET;
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/*
 * 页节点额外的布局 包含多少个 "单元"。一个单元是一个键/值对。
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// 叶子节点的主体是一个单元格的数组。每个单元格是一个键，后面是一个值（一个序列化的行）。
// 叶子节点的主体布局
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

/*
 * 内部节点头布局
 */
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

/*
 * 内部节点的主体布局
 * 主体是一个单元格数组，其中每个单元格包含一个子指针和一个键。每个键都应该是其左侧子项中包含的最大键
 */

const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_CHILD_SIZE;

#endif
