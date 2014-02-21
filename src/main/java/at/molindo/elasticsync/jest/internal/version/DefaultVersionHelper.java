package at.molindo.elasticsync.jest.internal.version;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class DefaultVersionHelper implements IVersionHelper {

	/**
	 * return QueryBuilders.queryString(query).defaultOperator(
	 * QueryStringQueryBuilder.Operator.AND) .defaultField("_all");
	 * 
	 * @param query
	 * 
	 * @return
	 */
	@Override
	public String createQuery(String q) {

		JsonObject queryString = new JsonObject();
		queryString.addProperty("query", q);
		queryString.addProperty("default_field", "_all");
		queryString.addProperty("default_operator", "AND");

		JsonObject query = new JsonObject();
		query.add("query_string", queryString);

		JsonObject o = new JsonObject();
		o.add("query", query);
		o.add("fields", new JsonArray());

		return o.toString();
	}

}
