package com.dudu.app.log.mr;

import java.util.Date;

import org.junit.jupiter.api.Test;

public class timeTest {

	@Test
	public void test() {
		
		Date date = new Date(1502686418952L+962*24*60*60*1000L);
		
		System.out.println(date);
		
	}
}
