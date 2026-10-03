// Deployments.ai modification: URL normalization and permutation helpers,
// moved verbatim out of crawl-redis.ts to keep that module below 700 lines.
import type { StoredCrawl } from "./crawl-redis";

export function normalizeURL(url: string, sc: StoredCrawl): string {
  const urlO = new URL(url);
  if (sc && sc.crawlerOptions && sc.crawlerOptions.ignoreQueryParameters) {
    urlO.search = "";
  }
  // allow hash-based routes
  if (
    !urlO.hash ||
    urlO.hash.length <= 2 ||
    (!urlO.hash.startsWith("#/") && !urlO.hash.startsWith("#!/"))
  ) {
    urlO.hash = "";
  }
  return urlO.href;
}

// For this function and the infrastructure surrounding it to work correctly, this function must:
// 1. Return the a non-zero number of permutations for all valid URLs.
//    generateURLPermutations(url).length > 0
// 2. The generated permutations of the returned array's members must be the same as the original generated permutations.
//    generateURLPermutations(url) == generateURLPermutations(generateURLPermutations(url)[n])
//    Obviously this is not valid in JS, but you get the idea.
// 3. Two generated permutations of signficantly different URLs may not have any overlap.
//    In practice, this means that if there is a generated array of permutations, there must be no URL that is
//     1. not included in that array, and
//     2. has a permutation that is included in that array.
//
// Points 1 and 2 are proven in permu-refactor.test.ts, point 3 is not as proving a negative is hard and outside the scope of a web crawler.
// - mogery
export function generateURLPermutations(url: string | URL): URL[] {
  const urlO = new URL(url);

  // Construct two versions, one with www., one without
  const urlWithWWW = new URL(urlO);
  const urlWithoutWWW = new URL(urlO);
  if (urlO.hostname.startsWith("www.")) {
    urlWithoutWWW.hostname = urlWithWWW.hostname.slice(4);
  } else {
    urlWithWWW.hostname = "www." + urlWithoutWWW.hostname;
  }

  let permutations = [urlWithWWW, urlWithoutWWW];

  // Construct more versions for http/https
  permutations = permutations.flatMap(urlO => {
    if (!["http:", "https:"].includes(urlO.protocol)) {
      return [urlO];
    }

    const urlWithHTTP = new URL(urlO);
    const urlWithHTTPS = new URL(urlO);
    urlWithHTTP.protocol = "http:";
    urlWithHTTPS.protocol = "https:";

    return [urlWithHTTP, urlWithHTTPS];
  });

  // Construct more versions for index.html/index.php
  permutations = permutations.flatMap(urlO => {
    const urlWithHTML = new URL(urlO);
    const urlWithPHP = new URL(urlO);
    const urlWithBare = new URL(urlO);
    const urlWithSlash = new URL(urlO);

    if (urlO.pathname.endsWith("/")) {
      urlWithBare.pathname =
        urlWithBare.pathname.length === 1
          ? urlWithBare.pathname
          : urlWithBare.pathname.slice(0, -1);
      urlWithHTML.pathname += "index.html";
      urlWithPHP.pathname += "index.php";
    } else if (urlO.pathname.endsWith("/index.html")) {
      urlWithPHP.pathname =
        urlWithPHP.pathname.slice(0, -"index.html".length) + "index.php";
      urlWithSlash.pathname = urlWithSlash.pathname.slice(
        0,
        -"index.html".length,
      );
      urlWithBare.pathname = urlWithBare.pathname.slice(
        0,
        -"/index.html".length,
      );
    } else if (urlO.pathname.endsWith("/index.php")) {
      urlWithHTML.pathname =
        urlWithHTML.pathname.slice(0, -"index.php".length) + "index.html";
      urlWithSlash.pathname = urlWithSlash.pathname.slice(
        0,
        -"index.php".length,
      );
      urlWithBare.pathname = urlWithBare.pathname.slice(
        0,
        -"/index.php".length,
      );
    } else {
      urlWithSlash.pathname += "/";
      urlWithHTML.pathname += "/index.html";
      urlWithPHP.pathname += "/index.php";
    }

    return [urlWithHTML, urlWithPHP, urlWithSlash, urlWithBare];
  });

  return [...new Set(permutations.map(x => x.href))].map(x => new URL(x));
}
