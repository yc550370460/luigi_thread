import luigi
import os
import requests
import logging
import json
import time
import threading
import bs4
from luigi_monitor import monitor

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
URL_LIST_DIR = os.path.join(CURRENT_DIR, "list")
IMG_LIST = os.path.join(CURRENT_DIR, "img")

log = logging.getLogger("test")

BASIC_URL = "http://photo.xitek.com/photoid/"
RANGE = 10
THREAD_COUNT = 2


class Save(luigi.Task):
    def requires(self):
        return Download()

    def run(self):
        with self.input().open("r") as f:
            img_list_tmp = json.load(f)

        print img_list_tmp


class Download(luigi.Task):
    def __init__(self):
        super(Download, self).__init__()
        if not os.path.exists(IMG_LIST):
            os.makedirs(IMG_LIST)
        self.img_list = list()

    def requires(self):
        return GetList()

    def output(self):
        return luigi.LocalTarget(os.path.join(IMG_LIST, "url_list.json"))

    def fetch(self, url):
        response = RequestProxy().request_get(url)
        bs_tmp = bs4.BeautifulSoup(response.content, "html.parser", from_encoding="utf-8")
        img_tag_list = bs_tmp.find_all("img")
        for item in img_tag_list:
            if item.get("class", None):
                if item.get("class")[0] == "mimg":
                    self.img_list.append(item["src"])

    def run(self):
        start_time = time.time()
        print start_time

        url_list_list = list()
        threading_groups = list()
        with self.input().open("r") as d:
            url_list = json.load(d)
        # use multiple threading
        for i in xrange(0, len(url_list), THREAD_COUNT):
            tmp = list()
            for j in xrange(THREAD_COUNT):
                if i + j < len(url_list):
                    tmp.append(url_list[i + j])
            url_list_list.append(tmp)
        for item in url_list_list:
            for h in item:
                threading_group = threading.Thread(target=self.fetch, args=(h, ))
                threading_group.setDaemon(True)
                threading_group.start()
                threading_groups.append(threading_group)
            for k in threading_groups:
                k.join()



        # not use multiple threading
        # for url in url_list:
        #     response = Request_proxy().request_get(url)
        #
        #     bs_tmp = bs4.BeautifulSoup(response.content, "html.parser", from_encoding="utf-8")
        #     img_tag_list =  bs_tmp.find_all("img")
        #     for item in img_tag_list:
        #         if item.get("class", None):
        #             if item.get("class")[0] == "mimg":
        #                 self.img_list.append(item["src"])
        with self.output().open("w") as f:
            json.dump(self.img_list, f, indent=4)
        print time.time() - start_time


class RequestProxy(object):
    @property
    def header(self):
        return {
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.76 Mobile Safari/537.36'
        }

    @property
    def proxy(self):
        return {'http': 'http://195.57.127.250:49590'}

    def request_get(self, url):
        try:
            s = requests.Session()
            print "Request: ", url
            response = s.get(url, proxies=self.proxy, headers=self.header)
            return response
        except Exception, e:
            raise e


class GetList(luigi.Task):
    def __init__(self):
        super(GetList, self).__init__()
        if not os.path.exists(URL_LIST_DIR):
            os.makedirs(URL_LIST_DIR)

    def output(self):
        return luigi.LocalTarget(os.path.join(URL_LIST_DIR, "url.json"))

    def run(self):
        url_list = list()
        for item in range(1, RANGE):
            try:
                response = RequestProxy().request_get(BASIC_URL + str(item))
            except Exception, e:
                raise e
            if response.status_code == 200:
                url_list.append(BASIC_URL + str(item))
            else:
                print "Error occur for url: %s, ", BASIC_URL + str(item)
                time.sleep(2)
        with self.output().open("w") as f:
            json.dump(url_list, f, indent=4)

if __name__ == "__main__":
    # create slack app and bound app link to the specified channel or people
    # https://api.slack.com/tutorials/slack-apps-hello-world
    with monitor(slack_url="https://hooks.slack.com/services/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", max_print=10, username="FooBar Monitor"):
        luigi.run(main_task_cls=Save)

