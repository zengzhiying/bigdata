<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">

<head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
    <title>站内搜索</title>
    <link href="http://so.huoduan.net/images/home.css" rel="stylesheet" type="text/css" />
    <script type="text/javascript" src="http://so.huoduan.net/js/jquery.min.js"></script>
    <script type="text/javascript" src="http://so.huoduan.net/js/main.js"></script>
    <script type="text/javascript">
        function subck() {
            var q = document.getElementById("kw").value;
            if (q == '' || q == '请输入关键字搜索网页') {
                return false;
            } else {
                return true;
            }
        }

        function toptab(obj, id) {
            $(".hothead a").removeClass("current");
            $("#tab" + id).addClass("current");
            $(".hotsearch ul").hide();
            $("#toplist" + id).show();
        }
        $(document).ready(function() {
            var WinH = $(window).height();
            var offset = $('#footer').offset();
            if (WinH > offset.top + $('#footer').height()) {
                var MH = WinH - offset.top - $('#footer').height() + 19;
                $('#footer').css("margin-top", MH.toString() + "px");
            }
            $('#footer').css("visibility", "visible");
        });
    </script>
</head>

<body>
    <div id="wrap">
        <div class="homelogo png" style="background:url(http://so.searcheasy.net/images/logo_index.gif) no-repeat center top">
            <h1>站内搜索</h1>
        </div>
        <div class="searchbox">
            <form action="/SolrSearch/s" method="get" onsubmit="return subck();">
                <input align="middle" name="q" class="q" id="kw" value="请输入关键字搜索网页" onfocus="javascript:if(this.value=='请输入关键字搜索网页'){this.value='';this.style.color='#333';this.style.borderColor='#FC8105';}" onblur="javascript:if(this.value==''){this.value='请输入关键字搜索网页';this.style.color='#CCC';this.style.borderColor='#CFC7C8';}"
                maxlength="100" size="50" autocomplete="off" />
                <input id="btn" class="btn" align="middle" value="搜索一下" type="submit" />
            </form>
        </div>
        <div id="footer" style=" visibility:hidden">Copyright &copy; <a href="">站内搜索</a>  <a href="#">京ICP备19191919号</a>  <span style="display:none;"></span>
        </div>
    </div>
</body>

</html>