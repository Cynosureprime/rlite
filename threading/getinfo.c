#ifdef _WIN
#include <windows.h>
int get_nprocs() {
    SYSTEM_INFO SysInfo;
    ZeroMemory(&SysInfo,sizeof(SYSTEM_INFO));
    GetSystemInfo(&SysInfo);
    return SysInfo.dwNumberOfProcessors;
}
#else
#include <unistd.h>
#ifdef _SC_NPROCESSORS_ONLN
#ifdef MACOSX
#include <sys/sysctl.h>
int get_nprocs() {
    int numCPUs;
    size_t len = sizeof(numCPUs);
    int mib[2] = { CTL_HW, HW_NCPU };
    if (sysctl(mib, 2, &numCPUs, &len, NULL, 0))
      return 1;
    return numCPUs;
}
#else
int get_nprocs() {
    int numCPUs;
    numCPUs = sysconf(_SC_NPROCESSORS_ONLN);
    if (numCPUs <=0)
      numCPUs = 1;
    return(numCPUs);
}
#endif
#endif

#ifdef SPARC
#ifndef AIX
int get_nprocs() { return(1); }
#endif
#endif
#ifdef _WIN32
#include <windows.h>

int get_nprocs() {
    SYSTEM_INFO SysInfo;
    ZeroMemory(&SysInfo,sizeof(SYSTEM_INFO));
    GetSystemInfo(&SysInfo);
    return SysInfo.dwNumberOfProcessors;
}
#endif


#endif
