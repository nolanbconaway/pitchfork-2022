# Pitchfork 2022

In 2017, as I was just stepping into the data science space, [I wrote a web scraper](https://nolanbconaway.github.io/blog/2017/pitchfork-roundup.html) which populated a SQLite database of Pitchfork reviews. That dataset became [quite popular on Kaggle](https://www.kaggle.com/nolanbconaway/pitchfork-data); as of now there are 1750 notebooks exploring the data.

More than five years have passed and I have gotten _a lot_ better at data modeling. I thought it might be fun to rewrite the scraper to see what I did differently, with the hopes of writing a blog post on the mistakes I naively made long ago.

This repo contains the scraper code as it stands; maybe one day I will add in the data. I call it `pitchfork-2022` because I will definitely not finish in 2021.


## TODO

- [ ] more pydantic assertions
- [ ] make flat view with casted year, etc, types for regular reviews
- [ ] better database assertions


# Data Model

![](schema.png)

# Data Build

All of the below relies on the specific structure of Pitchfork's webpages; I do not expect things to work as-is going forward.

## 1. Get a list of reviews on the website

- Script: `python -m scraper.get_pages`
- Writes: `_data/pages/*.json` 

This is done via a selenium scraper that iterates from page to page, storing the review URLs per page. It should be done overnight or some such because pitchfork often updates and the reviews per page will change. Luckily, once the page-to-url mapping is obtained, you are no longer in a race against time to capture data before things change. 

I ran it once overnight and it took 3h45m to scrape through 2015 pages.

The data should be produced in a form like

```json
{
    "page_scrape_ts_utc": "2021-12-12T01:45:01.438838",
    "page": 113,
    "urls": [
        "/reviews/albums/bellows-undercurrent/",
        "..."
    ]
}
```

## 2. Download the HTML for each of the URLs in the scraped page data

- Script: `python -m scraper.get_reviews_from_pages`
- Writes: `_data/reviews/*.json.gz` 

In this phase the JSON files per page are read and the URL key is iterated upon so that the HTML data for each review is saved. It would be better to obtain this info via XHR or some such structured data, but I was unable to find anything like that so we are in the business of scraping more HTML.

This step will be at least 10x slower than the previous step; one GET request needs to be executed per _review_, and there are 10 reviews per page. Luckily, review URLs shouldn't change and so there is no race against time to scrape the reviews before new ones are added. I recently ran it on ~24k reviews over the course of 1d21h without hitting any unrecoverable http errors.

A selenium browser is used to navigate to each URL and save whatever is under the `site-content` tag. Data are saved in gzipped json files like:

```json
{
    "url": "/reviews/albums/bellows-undercurrent/",
    "review_scrape_ts_utc": "2021-12-12T21:35:01.503821",
    "html": "..."
}
```

## 3. Build a SQLite database using the saved reviews

- Script: `python -m scraper.make_sqlite`
- Writes: `_data/data.sqlite3` 

This is the final step, which prepares the analytics-ready sqlite database. Each review's HTML is parsed via Beautifulsoup to extract out relevant info. 

I have it set up to run in chunks of <= 1000 URLs, depending on how many there are. The data are processed like:

```
for chunk in chunks
    reviews = parse HTML concurrently in N processes
    for review in reviews
        insert data into sqlite
```

I split into chunks because not _all_ computers are blessed with the memory that mine is.

This is _much_ faster than doing everything serially. I ran into database locking issues when doing everything concurrently; so this is probably the fastest option. In the current state it ran in ~10s with 32 processes on 24k reviews.

See the Data Model section for info on the schema.
