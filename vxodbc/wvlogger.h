
#ifndef __WVLOGGER_H
#define __WVLOGGER_H

#ifdef __cplusplus
extern "C" {
#endif

extern char log_level[2];
extern char log_moniker[255];

void wvlog_open();
void wvlog_print(const char *file, int line, const char *s);
void wvlog_close();
    
#ifdef __cplusplus
}
#endif

#endif // __WVLOGGER_H
