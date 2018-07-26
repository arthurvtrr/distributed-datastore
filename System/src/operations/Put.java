package operations;

public class Put implements Command<Object> {
	public Object key;
	public Object value;

	public Put(Object key, Object value) {
		this.key = key;
		this.value = value;
	}
}
