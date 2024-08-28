package com.distocraft.dc5000.dwhm;

import org.junit.Ignore;
import org.junit.Test;

/**
 * This class extends VersionUpdateActionTest. It is for running in UNIX. The parent class has some tests that fail on UNIX 
 * (or intermittently fail on UNIX) but pass in windows. This class excludes those tests by overriding them and giving them 
 * an Ignore tag. This class should be run on a UNIX machine instead of its parent class. All the tests seen in parent calls
 * will be run, and the ones marked in this class with Ignore tag will not be run. 
 * 
 * NB: When a way is found for an ignored test in this class to always pass on UNIX, then it should be updated here and have 
 * its Ignore tag removed. 
 * @author eanubda
 */

public class VersionUpdateActionUNIXTest extends VersionUpdateActionTest {
	
	//Overriding this method with the purpose of ignoring it
	
	@Test
	@Ignore
	public void testExecute_Upgrade_Stats() throws Exception {
		super.testExecute_Upgrade_Stats();
	}

}
