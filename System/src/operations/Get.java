package operations;

public class Get implements Query<Object> {
	public Object key;

	public Get(Object key) {
		this.key = key;
	}
}
