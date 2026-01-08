void set_options(
    uint32_t block_size,
    uint32_t target_sst_size,
    uint32_t num_memtable_limit,
    bool enable_wal
);

void open(char* path);
void close();
void put(char* key, char* val);
uint32_t get(char* key, char* buffer);
void remove(char* key);

bool detect_leaks();