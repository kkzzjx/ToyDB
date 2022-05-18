#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255


typedef struct{
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
}InputBuffer;


typedef enum{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
}MetaCommandResult;

typedef enum{
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIVE_ID,
    PREPARE_ERROR_FORMAT
}PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
    STATEMENT_UPDATE,
    STATEMENT_DELETE
}StatementType;

typedef enum{
    EXECUTE_SUCCESS,
    EXECUTE_ERROR,
    EXECUTE_TABLE_FULL,
    EXECUTE_DUPLICATE_KEY
}ExecuteResult;




typedef struct{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE+1];
    char email[COLUMN_EMAIL_SIZE+1];
    //C strings are supposed to end with a null character,so we should allocate one additional byte

}Row;


#define MAX_PAGE_NUM 100
typedef struct{
    int file_descriptor;
    uint32_t file_length;
    void* pages[MAX_PAGE_NUM];
    uint32_t num_pages;
}Pager;


typedef struct{
    uint32_t row_num;
   // void* page[MAX_PAGE_NUM];
   Pager* pager;
   uint32_t root_page_num;
}Table;

typedef struct{
    Table* table;
    // uint32_t row_num;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;
}Cursor;

// id sizes are not sure
#define size_of_attribute(Struct,Attribute) sizeof(((Struct*)0)->Attribute)
const uint32_t ID_SIZE=size_of_attribute(Row ,id);
const uint32_t USERNAME_SIZE=32;
const uint32_t EMAIL_SIZE=255;
const uint32_t ID_OFFSET=0;
const uint32_t USERNAME_OFFSET=ID_OFFSET+ID_SIZE;
const uint32_t EMAIL_OFFSET=USERNAME_OFFSET+USERNAME_SIZE;
const uint32_t ROW_SIZE=ID_SIZE+USERNAME_SIZE+EMAIL_SIZE;


const uint32_t PAGE_SIZE=4096;

#define TABLE_MAX_PAGES 100
const uint32_t ROW_NUM_PER_PAGE=PAGE_SIZE/ROW_SIZE;
const uint32_t TABLE_MAX_ROWS=ROW_NUM_PER_PAGE*TABLE_MAX_PAGES;

void serialize_row(Row* source,void* dest){
    memcpy(dest+ID_OFFSET,&(source->id),ID_SIZE);
    memcpy(dest+USERNAME_OFFSET,&(source->username),USERNAME_SIZE);
    memcpy(dest+EMAIL_OFFSET,&(source->email),EMAIL_SIZE);

}

void deserialize_row(void* source,Row* dest){
    memcpy(&(dest->id),source+ID_OFFSET,ID_SIZE);
    memcpy(&(dest->username),source+USERNAME_OFFSET,USERNAME_SIZE);
    memcpy(&(dest->email),source+EMAIL_OFFSET,EMAIL_SIZE);
}

typedef enum{
    NODE_INTERNAL,
    NODE_LEAF
}NodeType;

/**
 * Common Node Header Layout
 */

const uint32_t NODE_TYPE_SIZE=sizeof(uint8_t); //1 byte
const uint32_t NODE_TYPE_OFFSET=0;
const uint32_t IS_ROOT_SIZE=sizeof(uint8_t); //1 byte
const uint32_t IS_ROOT_OFFSET=NODE_TYPE_OFFSET+NODE_TYPE_SIZE;
const uint32_t PARENT_NODE_SIZE=sizeof(uint32_t);//4 bytes
const uint32_t PARENT_NODE_OFFSET=IS_ROOT_OFFSET+IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE=NODE_TYPE_SIZE+IS_ROOT_SIZE+PARENT_NODE_SIZE;

/**
 * Leaf Node Header Layout
 *
 */

const uint32_t LEAF_NODE_NUM_CELLS_SIZE=sizeof (uint32_t); //4 bytes
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET=NODE_TYPE_OFFSET+COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE=COMMON_NODE_HEADER_SIZE+LEAF_NODE_NUM_CELLS_SIZE;

/**
 * Leaf Node Body Header
 *
 */

const uint32_t LEAF_NODE_KEY_SIZE=sizeof(uint32_t);  //k  4bytes
const uint32_t LEAF_NODE_KEY_OFFSET=0;
const uint32_t LEAF_NODE_VALUE_SIZE=ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET=LEAF_NODE_KEY_SIZE+LEAF_NODE_KEY_OFFSET;
const uint32_t LEAF_NODE_CELL_SIZE=LEAF_NODE_KEY_SIZE+LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_FOR_CELL_SIZE=PAGE_SIZE-LEAF_NODE_HEADER_SIZE; // max space for cells
const uint32_t LEAF_NODE_MAX_NUM=LEAF_NODE_FOR_CELL_SIZE/LEAF_NODE_CELL_SIZE;// how many cells can be placed in one page

/**
 * Accessing Leaf Node Fields
 */

uint32_t* leaf_node_num_cells(void* node){
    return node+LEAF_NODE_NUM_CELLS_OFFSET;
}

void* leaf_node_cell(void* node,uint32_t cell_num){
    return node+LEAF_NODE_HEADER_SIZE+cell_num*LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void* node,uint32_t cell_num){
    return leaf_node_cell(node,cell_num);
}

void* leaf_node_value(void* node,uint32_t cell_num){
    return leaf_node_cell(node,cell_num)+LEAF_NODE_KEY_SIZE;
}
NodeType get_node_type(void *node){
    uint8_t value=*((uint8_t*)(node+NODE_TYPE_OFFSET));
    return (NodeType)value;
}

void set_node_type(void *node,NodeType type){
  //  printf("set\n");
    uint8_t value=type;
    *((uint8_t*)(node+NODE_TYPE_OFFSET))=value;
}

void initialize_leaf_node(void* node){

    *leaf_node_num_cells(node)=0; //num_cells=0;
    set_node_type(node,NODE_LEAF);
}




typedef struct {
    StatementType type;
    // data
    Row row;
}Statement;

InputBuffer* newInputBuffer(){
    InputBuffer* inputBuffer=(InputBuffer*)malloc(sizeof(InputBuffer));
    inputBuffer->buffer=NULL;
    inputBuffer->buffer_length=0;
    inputBuffer->input_length=0;

    return inputBuffer;
}

void read_line(InputBuffer* buffer){
    ssize_t read_size=getline(&(buffer->buffer),&(buffer->buffer_length),stdin);

    if(read_size<=0){
        printf("Error reading\n");
        exit(EXIT_FAILURE); //exit(1) 异常退出
    }

    buffer->input_length=read_size-1;//忽略换行符
    buffer->buffer[read_size-1]=0; //忽略换行符

}

void print_prompt() { 
    printf("zjxdb > "); 
}

void close_input_buffer(InputBuffer* buffer){
    free(buffer->buffer);//buffer数组释放
    free(buffer);//InputBuffer本身释放
}



void* get_page(Pager *pager,uint32_t page_num){
    if(page_num>MAX_PAGE_NUM){
        printf("Tried to fetch page number out of bounds\n");
        exit(EXIT_FAILURE);
    }

    if(pager->pages[page_num]==NULL){
        void* page= malloc(PAGE_SIZE);
        uint32_t num_pages=pager->file_length/PAGE_SIZE;

        if(pager->file_length%PAGE_SIZE!=0){
            num_pages++;
        }
        if(page_num<=num_pages){
            lseek(pager->file_descriptor,page_num*PAGE_SIZE,SEEK_SET);
            ssize_t read_bytes=read(pager->file_descriptor,page,PAGE_SIZE);
            if(read_bytes==-1){
                printf("Error reading file\n");
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num]=page;
        if(page_num>=pager->num_pages){
            pager->num_pages=page_num+1;
        }
    }

    return pager->pages[page_num];

}



Cursor* table_start(Table* table){
    Cursor *cursor= malloc(sizeof(Cursor));
    cursor->table=table;
    cursor->page_num=table->root_page_num;
    cursor->cell_num=0;

    void* root_node=get_page(table->pager,table->root_page_num);
    uint32_t num_cells= *leaf_node_num_cells(root_node);
    cursor->end_of_table=(num_cells==0);
    return cursor;
}

Cursor* table_end(Table* table){
    Cursor* cursor= malloc(sizeof (Cursor));
    cursor->table=table;
    cursor->page_num=table->root_page_num;
    void* page= get_page(table->pager,cursor->page_num);

    uint32_t num_cells=*leaf_node_num_cells(page);
    cursor->cell_num=num_cells;
    cursor->end_of_table=true;

    return cursor;
}



// you can get row's position from a page(get offset of row in this page)
void* cursor_value(Cursor* cursor){
    uint32_t page_num=cursor->page_num;
    void* page= get_page(cursor->table->pager,page_num);
    return leaf_node_value(page,cursor->cell_num);
}

//cell_num +1
void cursor_advance(Cursor* cursor){
    uint32_t page_num=cursor->page_num;
    void* page= get_page(cursor->table->pager,page_num);
    cursor->cell_num++;
    uint32_t num_cells=*leaf_node_num_cells(page);
    if(cursor->cell_num>=num_cells){
        cursor->end_of_table=true;
    }
}



PrepareResult prepare_insert(InputBuffer* inputBuffer,Statement* statement){

    statement->type=STATEMENT_INSERT;
    char* keyword= strtok(inputBuffer->buffer," ");
    char* key_id= strtok(NULL," ");
    char* username= strtok(NULL," ");
    char* email= strtok(NULL," ");
    char* addition= strtok(NULL," ");
    if(addition!=NULL) return PREPARE_ERROR_FORMAT;
    if(key_id==NULL||username==NULL||email==NULL){
        return PREPARE_SYNTAX_ERROR;
    }
    int id=atoi(key_id);
    if(id<0) return PREPARE_NEGATIVE_ID;
    if(strlen(username)>COLUMN_USERNAME_SIZE){
        return  PREPARE_STRING_TOO_LONG;
    }
    if(strlen(email)>COLUMN_EMAIL_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }

    statement->row.id=id;
    strcpy(statement->row.username,username);
    strcpy(statement->row.email,email);
    return PREPARE_SUCCESS;

}



// mini compiler
// prepare statement,and output statement
PrepareResult prepare_statement(InputBuffer* inputBuffer,Statement* statement){
    if(strncmp(inputBuffer->buffer,"insert",6)==0){
        return prepare_insert(inputBuffer,statement);


        //add some judgement
//        statement->type=STATEMENT_INSERT;
//        //input %d need sing & !!!(so easy to ignore)
//        int args_num= sscanf(inputBuffer->buffer,"insert %d %s %s",&(statement->row.id),statement->row.email,statement->row.username);
//        if(args_num<3){
//            return  PREPARE_SYNTAX_ERROR;
//        }
//        return PREPARE_SUCCESS;
    }
    if(strcmp(inputBuffer->buffer,"select")==0){


        statement->type=STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }


    return PREPARE_UNRECOGNIZED_STATEMENT;
}
void leaf_node_insert(Cursor* cursor,uint32_t key,Row* value){
    void* node= get_page(cursor->table->pager,cursor->page_num);
    uint32_t num_cell= *leaf_node_num_cells(node);
    if(num_cell>LEAF_NODE_MAX_NUM){
        printf("Need to implement splitting a leaf Node\n");
        exit(EXIT_FAILURE);
    }
    if(cursor->cell_num<num_cell){
        for(uint32_t i=num_cell;i>cursor->cell_num;i--){
            memcpy(leaf_node_cell(node,i), leaf_node_cell(node,i-1),LEAF_NODE_CELL_SIZE);
        }
    }
    *leaf_node_num_cells(node)+=1;
    *leaf_node_key(node,cursor->cell_num)=key;
    serialize_row(value, leaf_node_value(node,cursor->cell_num));
}



Cursor* leaf_node_find(Table* table,uint32_t page_num,uint32_t key){
    void* node= get_page(table->pager,page_num);
    uint32_t num_cells= *leaf_node_num_cells(node);
    Cursor* cursor= malloc(sizeof(Cursor));
    cursor->table=table;
    cursor->page_num=page_num;
    //cursor->cell_num ??
    //Binary Search
    // find key from [0,num_cells)
    uint32_t left=0,right=num_cells;
    while(left<right){
        uint32_t mid=(left+right)/2;
        uint32_t mid_key= *leaf_node_key(node,mid);
        if(mid_key==key){
            cursor->cell_num=mid;
            return cursor;
        }
        if(key<mid_key){
            right=mid;
        }
        else{
            left=mid+1;

        }
    }
    cursor->cell_num=left;
    return cursor;


}

Cursor* table_find(Table* table,uint32_t key){
    void* root_node= get_page(table->pager,table->root_page_num);
  //  printf("%d", get_node_type(root_node));
    if(get_node_type(root_node)==NODE_LEAF){
        return leaf_node_find(table,table->root_page_num,key);
    }
    else{
        printf("Internal Node\n");
        exit(EXIT_FAILURE);
    }

}

ExecuteResult execute_insert(Statement* statement,Table* table){

    void* page= get_page(table->pager,table->root_page_num);
    uint32_t num_cells= *leaf_node_num_cells(page);
    if(num_cells>=LEAF_NODE_MAX_NUM){
        return EXECUTE_TABLE_FULL;
    }


    Row *row=&(statement->row);
    uint32_t key=row->id;
    Cursor* cursor= table_find(table,key);

    if(cursor->cell_num<num_cells){
        uint32_t key_at_index=*leaf_node_key(page,cursor->cell_num);
        if(key==key_at_index){
            return EXECUTE_DUPLICATE_KEY;
        }
    }

    leaf_node_insert(cursor,row->id,row);
    return EXECUTE_SUCCESS;
}

void print_row(Row* row){
    printf("(%d %s %s)\n",row->id,row->username,row->email);
}

ExecuteResult execute_select_all(Statement* statement,Table* table){
    Cursor *cursor= table_start(table);
    uint32_t all_num=table->row_num;
    if(all_num<0) return EXECUTE_ERROR;
    Row row;
//    for(uint32_t i=0;i<all_num;i++){
//        deserialize_row(read_slot(table,i),&row);
//        print_row(&row);
//    }

    while(cursor->end_of_table!=true){
        deserialize_row(cursor_value(cursor),&row);
        print_row(&row);
        cursor_advance(cursor);
    }
    free(cursor);


    return EXECUTE_SUCCESS;


}

ExecuteResult execute_statement(Statement* statement,Table* table){
    switch (statement->type) {
        case(STATEMENT_INSERT):
           // printf("insert successfully!\n");
            return execute_insert(statement,table);
        case (STATEMENT_SELECT):
           // printf("select successfully!\n");
            return execute_select_all(statement,table);
        case(STATEMENT_DELETE):
            printf("do delete\n");
            break;
        case(STATEMENT_UPDATE):
            printf("do update\n");
            break;
    }
}

//Table *newTable(){
//    Table *table=(Table*) malloc(sizeof(Table));
//    table->row_num=0;
//    for(uint32_t i=0;i<TABLE_MAX_PAGES;i++){
//        table->page[i]=NULL;
//    }
//
//    return table;
//}
Pager *pager_open(const char * filename){
    int fd= open(filename,
                 O_RDWR |
                 O_CREAT,
                 S_IWUSR |
                 S_IRUSR
                 );
    if(fd==-1){
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length=lseek(fd,0,SEEK_END);

    Pager *pager=malloc(sizeof(Pager));
    pager->file_descriptor=fd;
    pager->file_length=file_length;
    pager->num_pages=file_length/PAGE_SIZE;
    if(file_length%PAGE_SIZE!=0){
        printf("Db file is not a whole number of pages.Corrupt file.\n");
        exit(EXIT_FAILURE);
    }

    for(uint32_t i=0;i<MAX_PAGE_NUM;i++){
        pager->pages[i]=NULL;
    }

    return pager;

}

Table *db_open(const char* filename){
    Pager* pager=pager_open(filename);
    Table *table=(Table*) malloc(sizeof (table));
    table->pager=pager;
    table->root_page_num=0;
    if(pager->num_pages==0){
        //new database file
        void* root_node= get_page(pager,0);
        initialize_leaf_node(root_node);
    }
    return table;
}



void pager_flush(Pager* pager,uint32_t page_num){
    //printf("test\n");
    if(pager->pages[page_num]==NULL){
        printf("Tried to flush null page!\n");
        exit(EXIT_FAILURE);
    }

//     off_t offset=lseek(pager->file_descriptor,page_num*PAGE_SIZE,SEEK_SET);
//    if(offset==-1){
//        printf("Error seeking!\n");
//        exit(EXIT_FAILURE);
//    }

    int write_bytes= write(pager->file_descriptor,pager->pages[page_num],PAGE_SIZE);
   // printf("test\n");
   // printf("%d\n",write_bytes);
    if(write_bytes==-1){
        printf("write failed!\n");
        exit(EXIT_FAILURE);
    }
}



void db_close(Table* table){
    Pager * pager=table->pager;
    uint32_t num_pages=pager->num_pages;
    for(uint32_t i=0;i<num_pages;i++){
        if(pager->pages[i]==NULL){
            continue;
        }
        pager_flush(pager,i);
        free(pager->pages[i]);
        pager->pages[i]=NULL;
    }




//    uint32_t full_page_num=row_num/ROW_NUM_PER_PAGE;
//
//
//    for(uint32_t i=0;i<full_page_num;i++){
//        if(pager->pages[i]==NULL){
//            continue;
//        }
//        pager_flush(pager,i,PAGE_SIZE);
//        free(pager->pages[i]);
//        pager->pages[i]=NULL;
//    }
//    uint32_t additional_rows=row_num%ROW_NUM_PER_PAGE;
//
//    if(additional_rows!=0){
//        if(pager->pages[full_page_num]!=NULL){
//            pager_flush(pager,full_page_num,additional_rows*ROW_SIZE);
//            free(pager->pages[full_page_num]);
//            pager->pages[full_page_num]=NULL;
//        }
//    }

    int close_result= close(pager->file_descriptor);
    if(close_result==-1){
        printf("Close file failed!\n");
        exit(EXIT_FAILURE);
    }

//    for(uint32_t i=0;i<MAX_PAGE_NUM;i++){
//        if(pager->pages[i]){
//            free(pager->pages[i]);
//            pager->pages[i]=NULL;
//        }
//    }

    free(pager);
    free(table);


}

void print_constants() {
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_FOR_CELLS: %d\n", LEAF_NODE_FOR_CELL_SIZE);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_NUM);
}

void print_leaf_node(void* node){
    uint32_t num_cells= *leaf_node_num_cells(node);
    printf("leaf(size %d)\n",num_cells);
    for(uint32_t i=0;i<num_cells;i++){
        uint32_t key= *leaf_node_key(node,i);
        printf("  - %d : %d\n", i, key);
    }
}

MetaCommandResult do_meta_command(InputBuffer* inputBuffer,Table* table){
    if(strcmp(inputBuffer->buffer,".exit")==0){
       // printf("test\n");
        db_close(table);
        exit(EXIT_SUCCESS);
    }
    else if(strcmp(inputBuffer->buffer,".constants")==0){
        printf("Constants:\n");
        print_constants();
        return META_COMMAND_SUCCESS;
    }
    else if(strcmp(inputBuffer->buffer,".btree")==0){
        printf("Tree:\n");
        print_leaf_node(get_page(table->pager,0));
        return META_COMMAND_SUCCESS;

    }
    else{
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}




int main(int argc,char* argv[]){
    if(argc<2){
        printf("Must supply a database filename\n");
        exit(EXIT_FAILURE);
    }
    char* filename=argv[1];
    printf("%s\n",filename);
    Table* table= db_open(filename);

    InputBuffer* inputBuffer=newInputBuffer();
    while(1){
        print_prompt();
        read_line(inputBuffer);

//        if(strcmp(inputBuffer->buffer,".exit")==0){
//            close_input_buffer(inputBuffer);
//            exit(EXIT_SUCCESS);//exit(0) 正常退出
//        }
//        else{
//            printf("command not found:%s\n",inputBuffer->buffer);
//        }
        if(inputBuffer->buffer[0]=='.'){
            MetaCommandResult result=do_meta_command(inputBuffer,table);
            switch (result) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n",inputBuffer->buffer);
                    continue;

            }
        }
        Statement statement;

        PrepareResult prepareResult=prepare_statement(inputBuffer,&statement);
        switch (prepareResult) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_SYNTAX_ERROR:
                printf("Syntax error. Could not parse statement\n");
                continue;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at start of '%s'\n",inputBuffer->buffer);
                continue;

            case PREPARE_STRING_TOO_LONG:
                printf("Your string too loooong!\n");
                continue;
            case PREPARE_NEGATIVE_ID:
                printf("Negative id!\n");
                continue;
            case PREPARE_ERROR_FORMAT:
                printf("Error format!\n");
                continue;
        }

        ExecuteResult executeResult=execute_statement(&statement,table);
        switch (executeResult) {
            case EXECUTE_ERROR:
                printf("Execute failed!\n");
                break;
            case EXECUTE_TABLE_FULL:
                printf("Error:Table full!\n");
                break;
            case EXECUTE_SUCCESS:
                printf("Executed!\n");
                break;
            case EXECUTE_DUPLICATE_KEY:
                printf("Error:Duplicate key.\n");
                break;
        }

       // printf("Executed\n");

    }
    db_close(table);



}