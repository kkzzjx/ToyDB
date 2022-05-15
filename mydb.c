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
    EXECUTE_TABLE_FULL
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
}Pager;


typedef struct{
    uint32_t row_num;
   // void* page[MAX_PAGE_NUM];
   Pager* pager;
}Table;

typedef struct{
    Table* table;
    uint32_t row_num;
    bool end_of_table;
}Cursor;

Cursor* table_start(Table* table){
    Cursor *cursor= malloc(sizeof(Cursor));
    cursor->table=table;
    cursor->row_num=0;
    cursor->end_of_table=(cursor->row_num==table->row_num);

    return cursor;
}

Cursor* table_end(Table* table){
    Cursor* cursor= malloc(sizeof (Cursor));
    cursor->table=table;
    cursor->row_num=table->row_num;
    cursor->end_of_table=true;

    return cursor;
}

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



const uint32_t PAGE_SIZE=4096;

#define TABLE_MAX_PAGES 100
const uint32_t ROW_NUM_PER_PAGE=PAGE_SIZE/ROW_SIZE;
const uint32_t TABLE_MAX_ROWS=ROW_NUM_PER_PAGE*TABLE_MAX_PAGES;

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
    }

    return pager->pages[page_num];

}

void cursor_advance(Cursor* cursor){
    cursor->row_num++;
    if(cursor->row_num>=cursor->table->row_num){
        cursor->end_of_table=true;
    }

}

// you can get row's position from a page(get offset of row in this page)
void* cursor_value(Cursor* cursor){

//    int page_id=row_num/ROW_NUM_PER_PAGE;
//    void* page=table->page[page_id];
//
//   if(page==NULL){
//       //insert
//       page=malloc(PAGE_SIZE);
//       table->page[page_id]=page;
//   }
    uint32_t row_num=cursor->row_num;


    uint32_t page_num=row_num/ROW_NUM_PER_PAGE;
    void* page=get_page(cursor->table->pager,page_num);
   uint32_t row_offset=row_num%ROW_NUM_PER_PAGE;
   uint32_t offset=row_offset*ROW_SIZE;
   return offset+page;
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

ExecuteResult execute_insert(Statement* statement,Table* table){
    Cursor *cursor= table_end(table);
    if(table->row_num>=TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }
    Row *row=&(statement->row);
    serialize_row(row, cursor_value(cursor));
    table->row_num++;
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
    for(uint32_t i=0;i<MAX_PAGE_NUM;i++){
        pager->pages[i]=NULL;
    }

    return pager;

}

Table *db_open(const char* filename){
    Pager* pager=pager_open(filename);
    uint32_t row_num=pager->file_length/ROW_SIZE;
  //printf("test:row_num %d\n",pager->file_length);
  //  printf("%d\n",pager->file_length);
    Table *table=(Table*) malloc(sizeof (table));
    table->row_num=row_num;
    table->pager=pager;
    return table;
}




void pager_flush(Pager* pager,uint32_t page_num,uint32_t size){
    //printf("test\n");
    if(pager->pages[page_num]==NULL){
        printf("Tried to flush null page!\n");
        exit(EXIT_FAILURE);
    }

     off_t offset=lseek(pager->file_descriptor,page_num*PAGE_SIZE,SEEK_SET);
    if(offset==-1){
        printf("Error seeking!\n");
        exit(EXIT_FAILURE);
    }

    int write_bytes= write(pager->file_descriptor,pager->pages[page_num],size);
   // printf("test\n");
   // printf("%d\n",write_bytes);
    if(write_bytes==-1){
        printf("write failed!\n");
        exit(EXIT_FAILURE);
    }
}



void db_close(Table* table){
    Pager * pager=table->pager;
    uint32_t row_num=table->row_num;
    uint32_t full_page_num=row_num/ROW_NUM_PER_PAGE;


    for(uint32_t i=0;i<full_page_num;i++){
        if(pager->pages[i]==NULL){
            continue;
        }
        pager_flush(pager,i,PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i]=NULL;
    }
    uint32_t additional_rows=row_num%ROW_NUM_PER_PAGE;

    if(additional_rows!=0){
        if(pager->pages[full_page_num]!=NULL){
            pager_flush(pager,full_page_num,additional_rows*ROW_SIZE);
            free(pager->pages[full_page_num]);
            pager->pages[full_page_num]=NULL;
        }
    }

    int close_result= close(pager->file_descriptor);
    if(close_result==-1){
        printf("Close file failed!\n");
        exit(EXIT_FAILURE);
    }

    for(uint32_t i=0;i<MAX_PAGE_NUM;i++){
        if(pager->pages[i]){
            free(pager->pages[i]);
            pager->pages[i]=NULL;
        }
    }

    free(pager);
    free(table);


}
MetaCommandResult do_meta_command(InputBuffer* inputBuffer,Table* table){
    if(strcmp(inputBuffer->buffer,".exit")==0){
       // printf("test\n");
        db_close(table);
        exit(EXIT_SUCCESS);
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
//            case EXECUTE_SUCCESS:
//                continue;
        }

       // printf("Executed\n");

    }
    db_close(table);



}