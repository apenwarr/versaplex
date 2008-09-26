
#ifndef __WVLOGGER_H
#define __WVLOGGER_H

#ifdef __cplusplus
extern "C" {
#endif

extern int log_level;

struct pstring
{
    char *const string;
    const int length;
};

struct pstring wvlog_get_moniker();
int wvlog_isset();
void wvlog_open();
void wvlog_print(const char *file, int line, const char *s);
void wvlog_close();
    
#ifdef __cplusplus
}
#endif

#endif // __WVLOGGER_H
