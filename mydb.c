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
    PREPARE_SYNTAX_ERROR
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
    EXECUTE_TABLE_FULL
}ExecuteResult;

typedef struct{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];

}Row;
#define MAX_PAGE_NUM 100

typedef struct{
    uint32_t row_num;
    void* page[MAX_PAGE_NUM];
}Table;
// id sizes are not sure
#define size_of_attribute(Struct,Attribute) sizeof(((Struct*)0)->Attribute)
const uint32_t ID_SIZE=size_of_attribute(Row ,id);
const uint32_t USERNAME_SIZE=32;
const uint32_t EMAIL_SIZE=255;
const uint32_t ID_OFFSET=0;
const uint32_t USERNAME_OFFSET=ID_OFFSET+ID_SIZE;
const uint32_t EMAIL_OFFSET=USERNAME_OFFSET+USERNAME_SIZE;
const uint32_t ROW_SIZE=ID_SIZE+USERNAME_SIZE+EMAIL_SIZE;




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

MetaCommandResult do_meta_command(InputBuffer* inputBuffer){
    if(strcmp(inputBuffer->buffer,".exit")==0){
        exit(EXIT_SUCCESS);
    }
    else{
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

const uint32_t PAGE_SIZE=4096;

#define TABLE_MAX_PAGES 100
const uint32_t ROW_NUM_PER_PAGE=PAGE_SIZE/ROW_SIZE;
const uint32_t TABLE_MAX_ROWS=ROW_NUM_PER_PAGE*TABLE_MAX_PAGES;


// you can get row's position from a page(get offset of row in this page)
void* read_slot(Table* table,uint32_t row_num){

    int page_id=row_num/ROW_NUM_PER_PAGE;
   void* page=table->page[page_id];

   if(page==NULL){
       //insert
       page=malloc(PAGE_SIZE);
   }
   uint32_t row_offset=row_num%ROW_NUM_PER_PAGE;
   uint32_t offset=row_offset*ROW_SIZE;
   return offset+page;
}



// mini compiler
// prepare statement,and output statement
PrepareResult prepare_statement(InputBuffer* inputBuffer,Statement* statement){
    if(strncmp(inputBuffer->buffer,"insert",6)==0){
        statement->type=STATEMENT_INSERT;
        //input %d need sing & !!!(so easy to ignore)
        int args_num= sscanf(inputBuffer->buffer,"insert %d %s %s",&(statement->row.id),statement->row.email,statement->row.username);
        if(args_num<3){
            return  PREPARE_SYNTAX_ERROR;
        }
        return PREPARE_SUCCESS;
    }
    if(strcmp(inputBuffer->buffer,"select")==0){
        statement->type=STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }


    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement* statement,Table* table){
    if(table->row_num>=TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }
    Row *row=&(statement->row);
    serialize_row(row, read_slot(table,table->row_num));
    table->row_num++;
    return EXECUTE_SUCCESS;
}

void print_row(Row* row){
    printf("(%d %s %s)\n",row->id,row->username,row->email);
}

ExecuteResult execute_select_all(Statement* statement,Table* table){
    uint32_t all_num=table->row_num;
    if(all_num<0) return EXECUTE_ERROR;
    Row row;
    for(uint32_t i=0;i<all_num;i++){
        deserialize_row(read_slot(table,i),&row);
        print_row(&row);
    }

    return EXECUTE_SUCCESS;


}

ExecuteResult execute_statement(Statement* statement,Table* table){
    switch (statement->type) {
        case(STATEMENT_INSERT):
            printf("insert successfully!\n");
            return execute_insert(statement,table);
        case (STATEMENT_SELECT):
            printf("select successfully!\n");
            return execute_select_all(statement,table);
        case(STATEMENT_DELETE):
            printf("do delete\n");
            break;
        case(STATEMENT_UPDATE):
            printf("do update\n");
            break;
    }
}

Table *newTable(){
    Table *table=(Table*) malloc(sizeof(Table));
    table->row_num=0;
    for(uint32_t i=0;i<TABLE_MAX_PAGES;i++){
        table->page[i]=NULL;
    }

    return table;
}

void freeTable(Table* table){
    for(uint32_t i=0;table->page[i];i++){
        free(table->page[i]);
    }
    free(table);
}




int main(){
    Table* table=newTable();
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
            MetaCommandResult result=do_meta_command(inputBuffer);
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
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at start of '%s'\n",inputBuffer->buffer);
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
//            case EXECUTE_SUCCESS:
//                continue;
        }

       // printf("Executed\n");

    }
    freeTable(table);



}