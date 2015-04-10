package ae.teletronics.cache.examples.dbversioncache;

public class StringValueContainer implements KeyValueOptimisticLockingDBWithPluggableCache.ValueContainer<String>, Cloneable {
	
	private Long version;
	private String text;
	
	public StringValueContainer(Long version, String text) {
		this.version = version;
		this.text = text;
	}

	@Override
	public void setVersion(Long newVersion) {
		version = newVersion;
	}

	@Override
	public Long getVersion() {
		return version;
	}

	@Override
	public String getValue() {
		return text;
	}
	
	@Override
	public StringValueContainer clone() {
		try {
			return (StringValueContainer)super.clone();
		} catch (CloneNotSupportedException e) {
			// Not gonna happen
			return null;
		}
	}
	
}
