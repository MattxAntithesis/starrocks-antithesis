# Lead L6: widen the race in the publish_log_version cancel callback (all-stop gdb).
# L6_ADDR (default 0x10e7a6e1) is the call to bthread_mutex_unlock(&response_mtx) in
# _submit_publish_log_version_task's {lambda()#2}::_M_invoke (cancel callback) in the
# 4.1.7 image binary, i.e. right AFTER latch.count_down() has returned. There the main
# thread (running ThreadPool::shutdown) calls usleep(L6_USEC) inside the inferior;
# during an inferior call gdb resumes ALL threads, so the RPC handler bthread can
# return and reuse its frame while the main thread is held before the unlock.
# Control: L6_ADDR=0x10e7a6d9 is the call to latch.count_down() in the same callback:
# the same pause, but BEFORE the count-down, while the handler is still blocked.
# (At the control address $rdi is &latch, not &response_mtx.)
set pagination off
set confirm off
handle SIGTERM nostop noprint pass
handle SIGPIPE nostop noprint pass
handle SIGUSR1 nostop noprint pass
handle SIGUSR2 nostop noprint pass
handle SIGSEGV stop print nopass
attach L6_PID
break *L6_ADDR
commands
silent
printf "L6BP before m=%p m->butex=%p *butex=%d\n", $rdi, *(void**)$rdi, **(int**)$rdi
call (int)usleep(L6_USEC)
printf "L6BP after  m=%p m->butex=%p\n", $rdi, *(void**)$rdi
continue
end
printf "L6_GDB_READY\n"
continue
printf "L6SIGNAL stopped: %d\n", $_siginfo.si_signo
bt 12
info registers rip rbx rdi r12
x/4gx $r12
signal SIGSEGV
