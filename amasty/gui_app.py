from tkinter import *
from tkinter import filedialog
from scrapy.utils import project
from scrapy import spiderloader
from tkinter import messagebox
from scrapy.utils.log import configure_logging
from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor
import threading

def get_spiders():
    settings = project.get_project_settings()
    spider_loader = spiderloader.SpiderLoader.from_settings(settings)
    return spider_loader.list()

def get_chosen_spider(i):
    global chosen_spider
    chosen_spider = i
    return chosen_spider


def execute_sd():
    settings = project.get_project_settings()
    configure_logging()
    crawler = CrawlerRunner(settings)
    crawler.crawl(chosen_spider)

    reactor.run(installSignalHandlers=False)

def start_threading(event):
    global execute_thread
    execute_thread = threading.Thread(target=execute_sd,daemon=True)
    execute_thread.start()
    app.after(10,check_thread)

def check_thread():
    if execute_thread.is_alive():
        app.after(10,check_thread)

#tkinter app
app = Tk()

#label
spider_label = Label(app,text='Choose a site')
spider_label.grid(row=0,column=0,sticky=W,pady=10,padx=10)

spider_text = StringVar(app)
spider_text.set('Choose a site')
spiders=[spider for spider in get_spiders()]

spider_drop = OptionMenu(app,spider_text,*spiders,command = get_chosen_spider)
spider_drop.grid(row=0,column=1,padx=1,columnspan=3)

#buttons


execute = Button(app,text='Search site',command=lambda:start_threading(None))
execute.grid(row=3,column=0,columnspan=4,pady=20)



app.title('Product scraper')
app.geometry('300x200')
app.resizable(False,False)
if __name__ =='__main__':
    app.mainloop()