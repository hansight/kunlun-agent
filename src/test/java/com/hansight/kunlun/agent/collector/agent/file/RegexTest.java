package com.hansight.kunlun.agent.collector.agent.file;

import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;
import oi.thekraken.grok.api.exception.GrokException;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Author:zhhui
 * DateTime:2014/8/4 14:12.
 */
public class RegexTest {
    @Test
    public void testRegex() throws GrokException {
       Grok grok = Grok.getInstance("%{IIS_LOG}");

        String log = "2013-12-16 00:26:42 W3SVC1 PBSZ-A 10.0.10.10 GET /CmbBank_GenShell/UI/GenShellPC/Login/&0.37762873293831944 - 443 - 117.136.39.222 HTTP/1.1 UCWEB/2.0+(Linux;+U;+Adr+4.2.2;+zh-CN;+HUAWEI+MT1-U06)+U2/1.0.0+UCBrowser/9.3.0.321+U2/1.0.0+Mobile WTFPC= https://pbsz.ebank.cmbchina.com/CmbBank_GenShell/UI/GenShellPC/Login/Login.aspx pbsz.ebank.cmbchina.com 404 0 2 210 985 0";
        Match gm = grok.match(log);
        System.out.println("gm = " + grok.getNamedRegex());
        gm.captures();
        System.out.println(gm.toJson());
        Pattern ptn=Pattern.compile("\\S*?((?<field36>[iI][dD]=(([0-9.]*-[0-9]*)|\\w*))\\S*)?");
      //  ptn= Pattern.compile(grok.getNamedRegex());
      Matcher m= ptn.matcher("WTFPC=:lv=1387153576339:ss=1387153576339;+CMB_GenServer=LoginType:A&BranchNo:&IdType:&CreditCardLoginType:PID;+AuthType=");
        if (m.matches()){
            System.out.println("field36 = " + m.group("field36"));
        }

    }
}
