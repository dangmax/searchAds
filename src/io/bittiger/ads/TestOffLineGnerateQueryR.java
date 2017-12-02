package io.bittiger.ads;

import java.io.File;

public class TestOffLineGnerateQueryR {

	public static void main(String[] args) {
		File synonymFile = new File("res/synonmy_0916.txt");
		File adsFile = new File("res/ads_0502.txt");
		QueryParser queryParser = new QueryParser();
		queryParser.OfflineQueryRewriteCreat(synonymFile, adsFile);
	}

}
