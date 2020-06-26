{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Monad.Extra (unfoldMapM)
import Control.Monad.Reader
import Data.Aeson
import qualified Data.ByteString.Lazy as BSL
import Data.Char (isDigit)
import Data.Foldable (traverse_)
import Data.List ((\\), nub)
import Data.Maybe (catMaybes, fromMaybe)
import Data.Text (Text, pack, unpack)
import GHC.Generics
import Network.HTTP.Req hiding (header)
import Options.Applicative
import System.Exit
import Text.HTML.Scalpel ((//), (@:), (@=), Scraper, URL, anySelector, attr, chroots, hasClass, scrapeURL, text)
import Text.ParserCombinators.ReadP

type RIO a = ReaderT Config IO a

newtype Args
  = Args {argsConfigPath :: FilePath}
  deriving (Show)

data Config
  = Config
      { configCraigslistUrl :: URL,
        configAirtableApiKey :: Text,
        configAirtableBaseId :: Text,
        configAirtableTableName :: Text
      }
  deriving (Show)

instance FromJSON Config where
  parseJSON = withObject "Config" $ \obj -> do
    craigslist <- obj .: "craigslist"
    url <- craigslist .: "url"
    airtable <- obj .: "airtable"
    apiKey <- airtable .: "api_key"
    baseId <- airtable .: "base_id"
    tableName <- airtable .: "table_name"
    return $ Config url apiKey baseId tableName

data Listing
  = Listing
      { listingUrl :: URL,
        listingTitle :: Text,
        listingPrice :: Int,
        listingLocation :: Text,
        listingMapsUrl :: Text
      }
  deriving (Show, Generic)

-- People tend to spam listings so we only care if the price has changed
instance Eq Listing where
  l == l' = listingTitle l == listingTitle l' && listingPrice l == listingPrice l'

instance FromJSON Listing where
  parseJSON = withObject "Listing" $ \obj -> do
    url <- obj .: "Link"
    title <- obj .: "Name"
    location <- obj .: "Neighborhood"
    price <- obj .: "Price"
    mapsUrl <- obj .: "Location"
    return $ Listing url title price location mapsUrl

instance ToJSON Listing where
  toJSON l =
    object
      [ "Link" .= listingUrl l,
        "Name" .= listingTitle l,
        "Neighborhood" .= listingLocation l,
        "Price" .= listingPrice l,
        "Location" .= listingMapsUrl l
      ]

data AirtableResponse
  = AirtableResponse
      { airtableResponseRecords :: [AirtableRecord],
        airtableResponseOffset :: Maybe Text
      }
  deriving (Show, Generic)

instance FromJSON AirtableResponse where
  parseJSON = withObject "AirtableListItem" $ \obj -> do
    records <- obj .: "records"
    offset <- obj .:? "offset"
    return $ AirtableResponse records offset

newtype AirtableRecord
  = AirtableRecord
      { airtableRecordFields :: Listing
      }
  deriving (Show, Generic)

instance FromJSON AirtableRecord where
  parseJSON = withObject "AirtableListRecordField" $ \obj -> do
    fields <- obj .: "fields"
    return $ AirtableRecord fields

instance ToJSON AirtableRecord where
  toJSON ai = object ["fields" .= airtableRecordFields ai]

data AirtablePage
  = AirtableFirst
  | AirtableOffset Text
  | AirtableLast

newtype AirtableInsert
  = AirtableInsert
      { airtableInsertRecords :: [AirtableRecord]
      }
  deriving (Show, Generic)

instance ToJSON AirtableInsert where
  toJSON ai = object ["records" .= airtableInsertRecords ai, "typecast" .= True]

airtableApiUrl :: RIO (Url 'Https, Option 'Https)
airtableApiUrl = do
  baseId <- asks configAirtableBaseId
  tableName <- asks configAirtableTableName
  apiKey <- asks configAirtableApiKey
  return (https "api.airtable.com" /: "v0" /: baseId /: tableName, "api_key" =: apiKey)

getAllListings :: RIO [Listing]
getAllListings = do
  rootUrl <- asks configCraigslistUrl
  urls <- lift $ getListingUrls rootUrl
  lift $ catMaybes <$> sequence (getListing <$> urls)

getListingUrls :: URL -> IO [URL]
getListingUrls url = fromMaybe [] <$> scrapeURL url urlScraper
  where
    urlScraper :: Scraper URL [URL]
    urlScraper = chroots ("a" @: [hasClass "hdrlnk"]) (attr "href" anySelector)

getListing :: URL -> IO (Maybe Listing)
getListing url = scrapeURL url listing
  where
    listing :: Scraper Text Listing
    listing = do
      location <- do
        rawLocation <- text $ ("span" @: [hasClass "postingtitletext"]) // "small"
        maybe (fail "couldn't parse location") return (parseLocation rawLocation)
      price <- do
        rawPrice <- text $ "span" @: [hasClass "price"]
        maybe (fail "couldn't parse price") return (parsePrice rawPrice)
      title <- text $ "span" @: ["id" @= "titletextonly"]
      lat <- attr "data-latitude" $ "div" @: ["id" @= "map"]
      lon <- attr "data-longitude" $ "div" @: ["id" @= "map"]
      return $ Listing url title price location ("https://www.google.com/maps/search/" <> lat <> "," <> lon)

parseLocation :: Text -> Maybe Text
parseLocation s = case readP_to_S locationParser (unpack s) of
  [(loc, _)] -> Just (pack loc)
  _ -> Nothing
  where
    locationParser :: ReadP String
    locationParser = skipSpaces >> between (char '(') (char ')') (munch (/= ')'))

parsePrice :: Text -> Maybe Int
parsePrice s = case readP_to_S priceParser (unpack s) of
  [(price, _)] -> Just price
  _ -> Nothing
  where
    priceParser :: ReadP Int
    priceParser = char '$' >> read <$> munch isDigit

getStoredListings :: RIO [Listing]
getStoredListings = do
  (url, params) <- airtableApiUrl
  runReq defaultHttpConfig $ unfoldMapM (fetchPage url params) AirtableFirst
  where
    fetchPage :: Url 'Https -> Option 'Https -> AirtablePage -> Req (Maybe ([Listing], AirtablePage))
    fetchPage _ _ AirtableLast = return Nothing
    fetchPage url params offset = do
      let offsetParam = case offset of
            AirtableOffset offset' -> "offset" =: offset'
            _ -> mempty
      resp <- responseBody <$> req GET url NoReqBody jsonResponse (params <> offsetParam)
      let newOffset = maybe AirtableLast AirtableOffset (airtableResponseOffset resp)
      let listings = airtableRecordFields <$> airtableResponseRecords resp
      return (Just (listings, newOffset))

storeListings :: [Listing] -> RIO ()
storeListings listings = do
  (url, params) <- airtableApiUrl
  runReq defaultHttpConfig $ do
    let body = AirtableInsert (AirtableRecord <$> listings)
    void $ req POST url (ReqBodyJson body) ignoreResponse params

chunks :: Int -> [a] -> [[a]]
chunks _ [] = []
chunks n xs = let (ys, zs) = splitAt n xs in ys : chunks n zs

logInfo :: MonadIO m => String -> m ()
logInfo = liftIO . putStrLn

main :: IO ()
main = do
  args <- execParser opts
  configFile <- BSL.readFile (argsConfigPath args)
  case eitherDecode configFile of
    Left err -> die $ "Failed to parse JSON config file: " <> err
    Right config -> flip runReaderT config $ do
      logInfo "Fetching new listings from Craigslist..."
      listings <- nub <$> getAllListings
      logInfo $ "Fetched " <> show (length listings) <> " new listings"
      logInfo "Fetching existing listings from Airtable..."
      storedListings <- getStoredListings
      logInfo $ "Fetched " <> show (length storedListings) <> " existing listings"
      let newListings = listings \\ storedListings
      logInfo $ "Saving " <> show (length newListings) <> " new listings..."
      traverse_ storeListings $ chunks 10 newListings
      logInfo "Done!"
  where
    opts :: ParserInfo Args
    opts =
      info
        (Args <$> strArgument (metavar "CONFIG_PATH" <> help "Path to the JSON configuration file") <**> helper)
        (fullDesc <> header "cl-to-airtable - Fetch listings from Craigslist and store them in Airtable")
