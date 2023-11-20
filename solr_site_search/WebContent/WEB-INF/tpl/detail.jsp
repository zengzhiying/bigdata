<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<%
	String status;
	String[] data = new String[3];
	if((request.getAttribute("result")) == null) {
		status = "没有数据";
	} else if(request.getAttribute("result").equals("error")) {
		status = "参数错误";
	} else {
		status = "";
		data = (String[]) request.getAttribute("result");
	}
%>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title><%
	if(status.equals("")) {
		out.print(data[1]);
	} else {
		out.print(status);
	}
%></title>
</head>
<body>

<%
	if(status.equals("")) {
		%>
		<h1 style="text-align:center;"><%=data[1] %></h1>
		<div style="margin:auto; width:960px; font-size:18px; font-family:微软雅黑;">
			<%=data[2] %>
		</div>
		<%
	} else {
		%>
		<h3><%=status %></h3>
		<%
	}
%>

</body>
</html>