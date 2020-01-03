import logging

from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By


def wait_load_finish(driver, class_name):
    """
    wait for loading finished, it will try twice for 15s
    :param driver:
    :return:
    """
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, class_name))
        )
    except:
        logging.warning(">>>> First 15s wait failed, PAGE SOURCE:")
        logging.warning(driver.page_source)
        logging.warning('!! >>>> >>>> first 15s wait failed, try 30s')
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CLASS_NAME, class_name))
        )


profile = webdriver.FirefoxProfile()
profile.set_preference("browser.cache.disk.enable", False)
profile.set_preference("browser.cache.memory.enable", False)
profile.set_preference("browser.cache.offline.enable", False)
profile.set_preference("network.http.use-cache", False)

firefox_options = webdriver.FirefoxOptions()
firefox_options.add_argument("--private")  # try to disable browser cache
# firefox_options.headless = True

# with webdriver.Firefox(firefox_profile=profile, options=firefox_options, executable_path='./geckodriver') as driver:
with webdriver.Firefox(firefox_profile=profile, options=firefox_options) as driver:
    driver.get("https://search.jd.com/")
    wait_load_finish(driver, 'copyright')

    driver.find_element_by_class_name("input_text").send_keys("abc")
    driver.find_element_by_class_name("input_submit").click()

    wait_load_finish(driver, 'copyright')