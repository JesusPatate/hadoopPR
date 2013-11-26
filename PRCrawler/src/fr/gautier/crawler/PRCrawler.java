package fr.gautier.crawler;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;


public class PRCrawler extends WebCrawler {
    
    private static File storageFolder;
    
    private final static Pattern FILTER1 = Pattern
            .compile("(.*(\\.(css|js|bmp|gif|jpe?g"
                    + "|png|tiff?|mid|mp2|mp3|mp4"
                    + "|wav|avi|mov|mpeg|ram|m4v|pdf"
                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|rss|ico))$)");
    
    private final static Pattern FILTER2 = Pattern
            .compile("(.*(\\.(css|js|bmp|gif|jpe?g"
                    + "|png|tiff?|mid|mp2|mp3|mp4"
                    + "|wav|avi|mov|mpeg|ram|m4v|pdf"
                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|rss|ico))\\?.*)");
    
    @Override
    public boolean shouldVisit(final WebURL url) {
        String href = url.getURL().toLowerCase();
        
        boolean visit = !(PRCrawler.FILTER1.matcher(href).matches()
                || PRCrawler.FILTER2.matcher(href).matches())
                && url.getDomain().equals("univ-nantes.fr");
        
        // if(visit == false) {
        // System.out.println("DBG lien ignoré :" + href); // DBG
        // }
        
        return visit;
    }
    
    @Override
    public void visit(final Page page) {
        String pageUrl = page.getWebURL().toString();
        
        pageUrl = this.cleanUrl(pageUrl);
        
        System.out.println("Visité : " + pageUrl); // DBG
        
        StringBuffer buf = new StringBuffer();
        buf.append(pageUrl);
        
        if (page.getParseData() instanceof HtmlParseData) {
            HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
            // String text = htmlParseData.getText();
            // String html = htmlParseData.getHtml();
            List<WebURL> links = htmlParseData.getOutgoingUrls();
            Set<String> outgoingLinks = new HashSet<String>();
            
            // On ne conserve pas les liens reflexifs
            outgoingLinks.add(pageUrl);
            
            for (WebURL wu : links) {
                String url = wu.getURL();
                
                if (!(PRCrawler.FILTER1.matcher(url).matches()
                        || PRCrawler.FILTER2.matcher(url).matches())
                        && wu.getDomain().equals("univ-nantes.fr")) {
                    
                    url = this.cleanUrl(url);
                    
                    // Suppression des doublons
                    if (outgoingLinks.add(url)) {
                        buf.append(" " + url);
                    }
                }
                else {
//                     System.out.println("Ignoré : " + wu.getURL()); // DBG
                }
            }
        }
        
        buf.append("\n");
        
        String filename = this.getMyId() + ".crawl";
        
        FileWriter fw;
        
        try {
            fw = new FileWriter(storageFolder + "/" + filename, true);
            fw.write(buf.toString());
            fw.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void configure(String storageFolderName) {
        
        storageFolder = new File(storageFolderName);
        
        if (!storageFolder.exists()) {
            storageFolder.mkdirs();
        }
    }
    
    private String cleanUrl(String url) {
        int ampIndex = url.indexOf('&');
        int qstMkIndex = url.indexOf('?');
        int index = 0;
        
        if(ampIndex > 0 || qstMkIndex > 0) {
            index = Math.min(ampIndex, qstMkIndex);
            
            if(index < 0) {
                index = Math.max(ampIndex, qstMkIndex);
            }
            
//            System.out.println("cleanURL : " + url + " -> " + url.substring(0, index)); // DBG
        }
        
        return (index > 0) ? url.substring(0, index) : url;
    }
}
