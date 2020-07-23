{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Monad.Extra (unfoldMapM)
import Control.Monad.Reader
import Data.Aeson
import Data.Aeson.Types (Parser, parseMaybe)
import qualified Data.ByteString.Lazy as BSL
import Data.Foldable (traverse_)
import Data.List ((\\), nub)
import Data.Maybe (catMaybes, fromMaybe)
import Data.Text (Text, pack, unpack)
import qualified Data.Vector as V
import Data.Void
import GHC.Generics (Generic)
import Network.HTTP.Req hiding (header)
import Options.Applicative hiding (Parser, some)
import System.Exit (die)
import Text.HTML.Scalpel ((//), (@:), (@=), Scraper, URL, anySelector, attr, chroots, hasClass, scrapeURL, text)
import Text.Megaparsec hiding (parseMaybe)
import qualified Text.Megaparsec as M
import Text.Megaparsec.Char
import Text.Read (readMaybe)

type RIO a = ReaderT Config IO a

type MParser a = Parsec Void Text a

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
    return $
      Config
        { configCraigslistUrl = url,
          configAirtableApiKey = apiKey,
          configAirtableBaseId = baseId,
          configAirtableTableName = tableName
        }

data Listing
  = Listing
      { listingUrl :: URL,
        listingId :: Text,
        listingName :: Text,
        listingPrice :: Int,
        listingNeighborhood :: Text,
        listingLat :: Text,
        listingLon :: Text
      }
  deriving (Show, Generic)

instance Eq Listing where
  l == l' =
    (listingId l == listingId l')
      || (listingName l == listingName l' && listingPrice l == listingPrice l')
      || (listingPrice l == listingPrice l' && listingLat l == listingLat l' && listingLon l == listingLon l')

listingLabelModifier :: String -> String
listingLabelModifier "listingId" = "ID"
listingLabelModifier "listingUrl" = "URL"
listingLabelModifier "listingName" = "Name"
listingLabelModifier "listingNeighborhood" = "Neighborhood"
listingLabelModifier "listingPrice" = "Price"
listingLabelModifier "listingLat" = "Lat"
listingLabelModifier "listingLon" = "Lon"
listingLabelModifier s = s

instance FromJSON Listing where
  parseJSON = genericParseJSON defaultOptions {fieldLabelModifier = listingLabelModifier}

instance ToJSON Listing where
  toJSON = genericToJSON defaultOptions {fieldLabelModifier = listingLabelModifier}

parseAirtableListResponse :: Value -> Parser ([Listing], AirtablePage)
parseAirtableListResponse = withObject "airtable list response" $ \obj -> do
  listings <- parseRecords =<< obj .: "records"
  offset <- maybe AirtableLast AirtableOffset <$> obj .:? "offset"
  return (listings, offset)
  where
    parseRecords :: Value -> Parser [Listing]
    parseRecords = withArray "airtable list response records" $ \arr ->
      catMaybes <$> traverse parseRecord (V.toList arr)
    parseRecord :: Value -> Parser (Maybe Listing)
    parseRecord = withObject "airtable list response record" $ \obj ->
      obj .: "fields" <|> return Nothing

data AirtablePage
  = AirtableFirst
  | AirtableOffset Text
  | AirtableLast

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
      id' <- do
        rawId <- text $ ("div" @: [hasClass "postinginfos"]) // ("p" @: [hasClass "postinginfo"])
        maybe empty return (parseId rawId)
      neighborhood <- do
        rawNeighborhood <- text $ ("span" @: [hasClass "postingtitletext"]) // "small"
        maybe empty return (parseNeighborhood rawNeighborhood)
      price <- do
        rawPrice <- text $ "span" @: [hasClass "price"]
        maybe empty return (parsePrice rawPrice)
      name <- text $ "span" @: ["id" @= "titletextonly"]
      (lat, lon) <- do
        rawLat <- attr "data-latitude" $ "div" @: ["id" @= "map"]
        rawLon <- attr "data-longitude" $ "div" @: ["id" @= "map"]
        return (rawLat, rawLon)
      return $
        Listing
          { listingId = id',
            listingUrl = url,
            listingName = name,
            listingPrice = price,
            listingNeighborhood = neighborhood,
            listingLat = lat,
            listingLon = lon
          }

parseId :: Text -> Maybe Text
parseId s = pack <$> M.parseMaybe parser s
  where
    parser :: MParser String
    parser = string "post id: " >> some digitChar

parseNeighborhood :: Text -> Maybe Text
parseNeighborhood s = pack <$> M.parseMaybe parser s
  where
    parser :: MParser String
    parser =
      let braces = between (char '(') (char ')')
       in space >> braces (some (anySingleBut ')'))

parsePrice :: Text -> Maybe Int
parsePrice s = read <$> M.parseMaybe parser s
  where
    parser :: MParser String
    parser = char '$' >> some digitChar

parseGeo :: (Text, Text) -> Maybe (Float, Float)
parseGeo (lat, lon) = do
  lat' <- readMaybe (unpack lat)
  lon' <- readMaybe (unpack lon)
  return (lat', lon')

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
            AirtableFirst -> mempty
            AirtableLast -> error "not possible"
      resp <- req GET url NoReqBody jsonResponse (params <> offsetParam)
      return $ responseBody resp >>= parseMaybe parseAirtableListResponse

storeListings :: [Listing] -> RIO ()
storeListings listings = do
  (url, params) <- airtableApiUrl
  runReq defaultHttpConfig $ do
    let records = [object ["fields" .= l] | l <- listings]
    let body = object ["records" .= records, "typecast" .= True]
    void $ req POST url (ReqBodyJson body) ignoreResponse params

chunks :: Int -> [a] -> [[a]]
chunks _ [] = []
chunks n xs = let (ys, zs) = splitAt n xs in ys : chunks n zs

logInfo :: String -> RIO ()
logInfo = liftIO . putStrLn

main :: IO ()
main = do
  args <- execParser opts
  configFile <- BSL.readFile $ argsConfigPath args
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
