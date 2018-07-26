package state;

import java.util.HashMap;
import java.util.Map;

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import operations.Delete;
import operations.Get;
import operations.Put;

public class KeyValueStore extends StateMachine {
	  private Map<Object, Commit> storage = new HashMap<>();

	  public Object put(Commit<Put> commit) {
	    Commit<Put> put = storage.put(commit.operation().key, commit);
	    return put == null ? null : put.operation().value;
	  }

	  public Object get(Commit<Get> commit) {
	    try {
	      Commit<Put> put = storage.get(commit.operation().key);
	      return put == null ? null : put.operation().value;
	    } finally {
	      commit.release();
	    }
	  }

	  public Object delete(Commit<Delete> commit) {
	    Commit<Put> put = null;
	    try {
	      put = storage.remove(commit.operation().key);
	      return put == null ? null : put.operation().value;
	    } finally {
	      if (put != null)
	        put.release();
	      commit.release();
	    }
	  }
	}

