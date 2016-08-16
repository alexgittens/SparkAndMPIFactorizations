# -- Machine type IA32
# mark_description "Intel(R) C++ Compiler for 32-bit applications, Version 9.0    Build 20050912Z %s";
# mark_description "-long_double -i-static -Qlocation,gld,/usr/intel/pkgs/gcc/3.4.2/bin -Qlocation,gas,/usr/intel/pkgs/gcc/3.4.2";
# mark_description "/bin -Wl,-rpath,/usr/intel/pkgs/gcc/3.4.2/lib -use_msasm -O3 -S";
	.ident "Intel(R) C++ Compiler for 32-bit applications, Version 9.0    Build 20050912Z %s"
	.ident "-long_double -i-static -Qlocation,gld,/usr/intel/pkgs/gcc/3.4.2/bin -Qlocation,gas,/usr/intel/pkgs/gcc/3.4.2/bin -Wl,-rpath,/u"
	.file "rdtsc.c"
	.text
# -- Begin  read_tsc
# mark_begin;
       .align    4,0x90
	.globl read_tsc

read_tsc:
..B1.1:                         # Preds ..B1.0
                                # LOE ebx ebp esi edi
..B1.5:                         # Preds ..B1.1
# Begin ASM
        rdtsc                                                   #4.13
    movl %eax,%ecx
    movl %edx,%eax
    shlq $32,%rax
    addq %rcx,%rax
# End ASM
                                # LOE ebx ebp esi edi
..B1.2:                         # Preds ..B1.5
        ret                                                     #8.1
        .align    4,0x90
                                # LOE
# mark_end;
	.type	read_tsc,@function
	.size	read_tsc,.-read_tsc
	.data
# -- End  read_tsc
	.data
	.section .note.GNU-stack, ""
# End
