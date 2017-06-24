package org.ht.jleveldb;

import java.util.List;

import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.Long0;
import org.ht.jleveldb.util.Object0;
import org.ht.jleveldb.util.Slice;

/**
 * An implementation of Env that forwards all calls to another Env.
 * May be useful to clients who wish to override just part of the
 * functionality of another Env.
 * 
 * @author thuang
 */
public class EnvWrapper implements Env {
	Env target;
	
	public EnvWrapper(Env env) {
		target = env;
	}
	
	public Env target() {
		return target;
	}
	
	@Override
	public Status newSequentialFile(String fname, Object0<SequentialFile> result) {
		return target.newSequentialFile(fname, result);
	}

	@Override
	public Status newRandomAccessFile(String fname, Object0<RandomAccessFile0> result) {
		return target.newRandomAccessFile(fname, result);
	}

	@Override
	public Status newWritableFile(String fname, Object0<WritableFile> result) {
		return target.newWritableFile(fname, result);
	}

	@Override
	public Status newAppendableFile(String fname, Object0<WritableFile> result) {
		return target.newAppendableFile(fname, result);
	}

	@Override
	public boolean fileExists(String fname) {
		return target.fileExists(fname);
	}

	@Override
	public Status getChildren(String dir, List<String> result) {
		return target.getChildren(dir, result);
	}

	@Override
	public Status deleteFile(String fname) {
		return target.deleteFile(fname);
	}

	@Override
	public Status createDir(String dirname) {
		return target.createDir(dirname);
	}

	@Override
	public Status deleteDir(String dirname) {
		return target.deleteDir(dirname);
	}

	@Override
	public Status getFileSize(String fname, Long0 fileSize) {
		return target.getFileSize(fname, fileSize);
	}

	@Override
	public Status renameFile(String src, String dest) {
		return target.renameFile(src, dest);
	}

	@Override
	public Status lockFile(String fname, Object0<FileLock0> lock) {
		return target.lockFile(fname, lock);
	}

	@Override
	public Status unlockFile(FileLock0 lock) {
		return target.unlockFile(lock);
	}
	
	@Override
	public void schedule(Runnable r) {
		target.schedule(r);
	}

	@Override
	public void startThread(Runnable runnable) {
		target.startThread(runnable);
	}

	@Override
	public Status newLogger(String fname, Object0<Logger0> logger) {
		return target.newLogger(fname, logger);
	}

	@Override
	public long nowMillis() {
		return target.nowMillis();
	}

	@Override
	public void sleepForMilliseconds(int millis) {
		target.sleepForMilliseconds(millis);
	}

	@Override
	public Status writeStringToFile(Slice data, String fname) {
		return target.writeStringToFile(data, fname);
	}
	
	@Override
	public Status writeStringToFileSync(Slice data, String fname) {
		return target.writeStringToFileSync(data, fname);
	}

	@Override
	public Status readFileToString(String fname, ByteBuf data) {
		return target.readFileToString(fname, data);
	}
}
