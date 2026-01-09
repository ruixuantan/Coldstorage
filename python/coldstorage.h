void cs_set_options(
    uint32_t block_size,
    uint32_t target_sst_size,
    uint32_t num_memtable_limit,
    bool enable_wal
);

void cs_open(char* path);
void cs_close();
void cs_put(char* key, char* val);
uint32_t cs_get(char* key, char* buffer);
void cs_remove(char* key);

void* cs_scan(char* lower, char* upper);
void cs_iterator_deinit(void* iterator);
uint32_t cs_iterator_key(void* iterator, char* buffer);
uint32_t cs_iterator_val(void* iterator, char* buffer);
bool cs_iterator_is_valid(void* iterator);
void cs_iterator_next(void* iterator);

bool cs_detect_leaks();