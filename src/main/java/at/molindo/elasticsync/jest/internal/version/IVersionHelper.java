package at.molindo.elasticsync.jest.internal.version;

public interface IVersionHelper {

	public static final IVersionHelper DEFAULT = new DefaultVersionHelper();

	/**
	 * return QueryBuilders.queryString(query).defaultOperator(
	 * QueryStringQueryBuilder.Operator.AND) .defaultField("_all");
	 * 
	 * @param query
	 * 
	 * @return
	 */
	String createQuery(String query);

}
