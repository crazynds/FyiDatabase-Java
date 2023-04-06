package engine.info;

public final class Parameters {


	public static long BLOCK_LOADED=0;
	public static long BLOCK_SAVED=0;

	public static long IO_READ_TIME=0;
	public static long IO_WRITE_TIME=0;
	public static long IO_SYNC_TIME=0;
	
	public static long IO_SEEK_READ_TIME=0;
	public static long IO_SEEK_WRITE_TIME=0;

	public static long MEMORY_ALLOCATED_BY_BLOCKS = 0;
	public static long MEMORY_ALLOCATED_BY_DIRECT_BLOCKS = 0;
	public static long MEMORY_ALLOCATED_BY_INDIRECT_BLOCKS = 0;
	public static long MEMORY_ALLOCATED_BY_RECORDS = 0;
	public static long MEMORY_ALLOCATED_BY_COMMITTABLE_BLOCKS = 0;
	
	public static long MEMORY_ALLOCATED_BY_BYTE_ARRAY = 0;
	
}
