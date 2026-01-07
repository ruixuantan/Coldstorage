void open(char* path);

void close();

bool execute(char* sql);

void list_tables();

bool display_table(char* table_name);

bool fetch();

void close_cursor();

void cursor_write_schema();

char* get_buffer();

int32_t get_buffer_len();

bool detect_leaks();