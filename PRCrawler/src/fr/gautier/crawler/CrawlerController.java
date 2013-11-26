package fr.gautier.crawler;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;


public class CrawlerController {
    
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Needed parameters: ");
            System.out.println("\t- dataFolder : dossier dans lequel seront "
                    + "stockées des données intermédiaires nécessaires "
                    + "aux crawlers");
            System.out.println("\t- nbCralwers : nombre de crawlers");
            System.out.println("\t- outputFolder : dossier dans lequel seront "
                    + "enregistrées les données des pages crawlées");
            return;
        }
        
        String dataFolder = args[0];
        
        int nbCralwers = Integer.parseInt(args[1]);
        
        String outputFolder = args[2];
        
        CrawlConfig config = new CrawlConfig();
        
        config.setCrawlStorageFolder(dataFolder);
        
        config.setPolitenessDelay(1000);
        config.setMaxDepthOfCrawling(-1);
        config.setMaxPagesToFetch(100);
        config.setResumableCrawling(false);
        
        PageFetcher pageFetcher = new PageFetcher(config);
        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
        RobotstxtServer robotstxtServer =
                new RobotstxtServer(robotstxtConfig, pageFetcher);
        CrawlController controller =
                new CrawlController(config, pageFetcher, robotstxtServer);
        
        controller.addSeed("http://www.univ-nantes.fr/");
        controller.addSeed("http://www.droit1.univ-nantes.fr/");
        controller.addSeed("http://www.medecine.univ-nantes.fr/");
        controller.addSeed("http://www.sciences-techniques.univ-nantes.fr/");
        
        PRCrawler.configure(outputFolder);
        
        controller.start(PRCrawler.class, nbCralwers);
    }
}
