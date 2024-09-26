package com.strandls.esmodule;

/**
 * @author Abhishek Rudra
 *
 */

public class ApiConstants {

	private ApiConstants() {
	}

	// versioning

	public static final String V1 = "/v1";

	// paths
	// ESController
	public static final String PING = "/ping";
	public static final String SERVICES = "/services";
	public static final String DATA = "/data";
	public static final String BULK_UPLOAD = "/bulk-upload";
	public static final String BULK_UPDATE = "/bulk-update";
	public static final String TERM_SEARCH = "/term-search";
	public static final String RANGE_SEARCH = "/range-search";
	public static final String GEOHASH_AGGREGATION = "/geohash-aggregation";
	public static final String TERMS_AGGREGATION = "/terms-aggregation";
	public static final String AGGREGATION = "/aggregation";
	public static final String MONTH_AGGREGATION = "/month-aggregation";
	public static final String BOUNDS = "/bounds";
	public static final String SEARCH = "/search";
	public static final String DOWNLOAD = "/download";
	public static final String MAPPING = "/mapping";
	public static final String ESMAPPING = "/createindex";
	public static final String INDEX_ADMIN = "/index-admin";
	public static final String RIGHTPAN = "/rightpan";
	public static final String NEARBY = "/nearby";
	public static final String AUTOSUGGEST = "/suggest";
	public static final String MATCHPHRASE = "/exactmatch";
	public static final String AUTOCOMPLETE = "/auto-complete";

	public static final String GETTOPUSERS = "/leaderboard";
	public static final String GETUSERSCORE = "/userscore";
	public static final String USERIBP = "/userIbp";
	public static final String REINDEX = "/reindex";
	public static final String FILTERAUTOCOMPLETE = "/filterautocomplete";
	public static final String LIST = "/list";
	public static final String FILTERS = "/filters";
	public static final String SPECIES = "/species";
	public static final String FORCEUPDATE = "forceupdateindex";
	public static final String FETCHINDEX = "fetchindex";
	public static final String USERINFO = "/userInfo";
	public static final String UPLOADERSINFO = "/uploadersInfo";
	public static final String IDENTIFIERSINFO = "/identifiersInfo";
	// binning Controller
	public static final String BINNING = "/binning";
	public static final String SQUARE = "/square";

	// geo Controller
	public static final String GEO = "/geo";
	public static final String WITHIN = "/within";

}
