package operations;

public class Delete implements Command<Object> {
	public Object key;

	public Delete(Object key) {
		this.key = key;
	}
}