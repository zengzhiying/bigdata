<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ page import="bean.Article" %>
<%@ page import="java.util.*" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">

<head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
    <title>${query } - 站内搜索</title>
    <script type="text/javascript" src="http://so.huoduan.net/js/jquery.min.js"></script>
    <script type="text/javascript" src="http://so.huoduan.net/js/main.js"></script>
    <link href="http://so.huoduan.net/images/style.css" rel="stylesheet" type="text/css" />
</head>

<body>
    <div id="header">
        <div class="con">
            <div class="logo png" style="background:url(resources/images/logo.png) no-repeat"><a href="/SolrSearch">站内搜索</a>
            </div>
            <div class="searchbox">
                <form action="/SolrSearch/s" method="get">
                    <input align="middle" name="q" class="q" id="kw" maxlength="100" size="50" autocomplete="off" value="${query }" />
                    <input id="btn" class="btn" align="middle" value="搜索一下" type="submit" />
                </form>
            </div>
        </div>
    </div>
    <!--header-->
    
    <%
    	int size;
    	List<Article> data = new ArrayList<Article>();
    	if(request.getAttribute("result") == null) {
    		size = 0;
    	} else {
    		data = (List<Article>) request.getAttribute("result");
    		size = data.size();
    		
    	}
    %>
    
    <div id="hd_main">
        <div id="res" class="res">
            <div id="resinfo">为您找到"
                <h1>${query }</h1>"相关结果约<%=size %>个</div>
            <div id="result">
            	<%
            		for(Article a:data) {
            			%>
            			<div class="g">
		                    <h2><a href="/SolrSearch/detail/<%=a.getId() %>" target="_blank" class="s" rel="nofollow"><b></b><%=a.getTitle() %></a></h2>
		                    <div class="std"><b></b><%=a.getAbs() %></div><span class="a">/SolrSearch/detail/<%=a.getId() %></span>
		                </div>
            			<%
            		}
            	%>
            </div>
            <div class="cl"></div>
        </div>
        <!--res-->
    </div>
    <!--main-->
    <div id="footer">Copyright &copy; <a href="">站内搜索</a>  <a href="#">京ICP备19191919号</a>  <span style="display:none;"></span>
    </div>
</body>

</html>