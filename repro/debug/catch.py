import gdb
print("GDB_READY", flush=True)
gdb.execute("set pagination off")
gdb.execute("set confirm off")
gdb.execute("handle all nostop noprint pass")
gdb.execute("handle SIGSEGV stop print nopass")
inf = gdb.selected_inferior()
def q(a):
    return int.from_bytes(bytes(inf.read_memory(a, 8)), 'little')
def sym(a):
    try:
        return gdb.execute("info symbol 0x%x" % a, to_string=True).strip()
    except Exception as e:
        return "?"
def col(p, depth=0):
    if p == 0:
        return "NULL"
    try:
        words = [q(p + 8*k) for k in range(8)]
    except Exception as e:
        return "unreadable %x" % p
    s = "%x vt=%s words=%s" % (p, sym(words[0]), " ".join("%x" % w for w in words))
    return s
def chunk(name, c):
    b, e = q(c), q(c+8)
    n = (e - b)//8
    print("%s chunk @%x columns=%d" % (name, c, n))
    for k in range(n):
        print("   [%d] %s" % (k, col(q(b+8*k))))
resume = "continue"
n = 0
while True:
    try:
        gdb.execute(resume)
    except gdb.error as e:
        print("resume error", e); break
    bt = gdb.execute("bt 14", to_string=True)
    if "rolling_append_selective" in bt or "MemTable" in bt:
        print("=== TARGET SIGSEGV ===")
        print(bt)
        print(gdb.execute("p $_siginfo._sifields._sigfault.si_addr", to_string=True))
        print(gdb.execute("info registers rip rdi rsi rdx rcx r8 r12 r13 r14 r15 rax rbx", to_string=True))
        print(gdb.execute("x/3i $pc", to_string=True))
        gdb.execute("frame 1")
        print(gdb.execute("info registers rip r12 r13 r14 r15 rbx", to_string=True))
        r12 = int(gdb.parse_and_eval("$r12")) & (2**64-1)
        r13 = int(gdb.parse_and_eval("$r13")) & (2**64-1)
        r14 = int(gdb.parse_and_eval("$r14")) & (2**64-1)
        print("loop index i =", r12)

        def strs(addr, ln=0x100):
            try:
                b = bytes(inf.read_memory(addr, ln))
            except Exception:
                return []
            out=[]; cur=b''
            for ch in b:
                if 32 <= ch < 127: cur += bytes([ch])
                else:
                    if len(cur) >= 3: out.append(cur.decode())
                    cur=b''
            return out
        # src data column of the faulting NullableColumn
        srcnull = q(q(r13) + 8*r12)
        dcol = q(srcnull + 16)
        print("faulting src NullableColumn %x data_col %s" % (srcnull, col(dcol)))
        gdb.execute("frame 2")
        mt = int(gdb.parse_and_eval("$rbx")) & (2**64-1)
        print("MemTable this=%x tablet_id=%d" % (mt, q(mt+0x50)))
        sch = q(mt+0x58); sd = q(mt+0x60)
        fb, fe = q(sch), q(sch+8)
        print("schema @%x num_fields=%d" % (sch, (fe-fb)//16))
        for k in range((fe-fb)//16):
            f = q(fb+16*k)
            print("   field[%d] @%x strs=%s" % (k, f, strs(f, 0x60)))
            for w in range(12):
                v = q(f+8*w)
                if 0x10000 < v < 0x7fffffffffff:
                    ss = strs(v, 24)
                    if ss: print("      +%x -> %x %s" % (8*w, v, ss[:2]))
        sb, se = q(sd), q(sd+8)
        print("slot_descs @%x size=%d" % (sd, (se-sb)//8))
        for k in range((se-sb)//8):
            s_ = q(sb+8*k)
            names=[]
            for w in range(24):
                v = q(s_+8*w)
                if 0x10000 < v < 0x7fffffffffff:
                    ss = strs(v, 24)
                    if ss: names.append(("+%x" % (8*w), ss[:1]))
            print("   slot[%d] @%x inline=%s ptrs=%s" % (k, s_, strs(s_, 0xc0), names))
        chunk("DEST(_result_chunk)", r14)
        chunk("SRC(_chunk)", r13)
        for fr in range(2, 6):
            try:
                gdb.execute("frame %d" % fr)
                print(gdb.execute("info registers rip rbx r12 r13 r14 r15", to_string=True))
            except Exception as e:
                print("frame", fr, e)
        print("=== DONE ===")
        break
    n += 1
    print("non-target SIGSEGV #%d" % n); print(bt)
    resume = "signal SIGSEGV"
