package controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import bean.Article;
import model.GetUrlParam;
import model.QueryModel;

@Controller("SolrSearch")
public class SolrSearch {
	
	@RequestMapping({"/s"})
	public ModelAndView search(HttpServletRequest request, HttpServletResponse response) {
		if(request.getParameter("q") == null || request.getParameter("q").equals("")) {
			try {
				response.sendRedirect("/SolrSearch");
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			String query = request.getParameter("q");
			ModelAndView mv = new ModelAndView();
			mv.addObject("query", query);
			//solr查询
			List<Article> result = new ArrayList<Article>();
			if((result = QueryModel.query(query)) == null) {
				mv.addObject("result", null);
			} else {
				mv.addObject("result", result);
			}
			
			mv.setViewName("list");
			return mv;
		}
		
		return null;
	}
	
	
	@RequestMapping({"/detail/*","/detail"})
	public String detail(HttpServletRequest request, HttpServletResponse response) {
		
		String url = request.getServletPath();
		
		String param = GetUrlParam.getParam(url);
		
		//查询
		if(param.equals("error")) {
			request.setAttribute("result", "error");
		} else {
			String[] result = QueryModel.queryOne(param);
			if(result == null) {
				request.setAttribute("result", null);
			} else {
				request.setAttribute("result", result);
				
			}
		}
		
		return "detail";
		
	}
	
	@RequestMapping("/")
	public String index() {
		return "index";
	}
	
}
