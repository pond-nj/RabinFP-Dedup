javac MyDedup.java
# java MyDedup upload 1048576 1048576 1048576 1048576 $1
java MyDedup upload 32 2048 4096 257 $1
java MyDedup download $1 "downloaded"
diff $1 "downloaded"