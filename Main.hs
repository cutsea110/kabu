{-# LANGUAGE OverloadedStrings #-}
module Main where

import Data.Foldable (find)
import Data.List (unfoldr)
import Control.Applicative ((<$>), (<*>), (<*), (*>), (<|>), many)
import Data.Attoparsec.Text (parseOnly, many', string, try, (<?>), decimal, char, Parser, notChar, double)
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.IO as T
import Data.Text.Lazy.Encoding (decodeUtf8)
import Data.Time.Calendar (Day, fromGregorian)
import Database.PostgreSQL.Simple (connect, Connection, defaultConnectInfo, ConnectInfo(..), execute, query, Only(..))
import Database.PostgreSQL.Simple.FromRow (FromRow(..), field)
import Database.PostgreSQL.Simple.ToField (ToField(..))
import Database.PostgreSQL.Simple.ToRow (ToRow(..))
import Network.HTTP (simpleHTTP, getRequest, getResponseBody)
import System.Environment (getArgs)
import System.IO (hClose)
import System.Process (runInteractiveProcess)

data Brand = Brand
             { brandCode :: Text
             , brandName :: Text
             , brandMarket :: Text
             , brandCategory :: Text
             , brandLastUpdated :: Day
             }
             deriving Show

data BrandKey = BrandKey { brandKeyCode :: Text, brandKeyMarket :: Text } deriving Show

data Stock = Stock
             { stockDay :: Day
             , stockCode :: Text
             , stockName :: Text
             , stockMarket :: Text
             , stockCategory :: Text
             , stockOpeningPrice :: Maybe Double
             , stockHighPrice :: Maybe Double
             , stockLowPrice :: Maybe Double
             , stockClosingPrice :: Maybe Double
             , stockVolumeOfTrading :: Maybe Double
             , stockTradingValue :: Maybe Double
             }
           deriving Show

text :: Parser Text
text = many (notChar ',') >>= return . T.pack
code, name, market, category :: Parser Text
code = text
name = text
market = text
category = text
maybeDouble :: Parser (Maybe Double)
maybeDouble = fmap return double <|> fmap failure (char '-')
  where failure = const $ fail "read '-'"

comma :: Parser Char
comma = char ','
dash :: Parser Char
dash = char '-'

stock :: Day -> Parser Stock
stock d = Stock d <$> code
                  <*> (comma *> name)
                  <*> (comma *> market)
                  <*> (comma *> category)
                  <*> (comma *> maybeDouble)
                  <*> (comma *> maybeDouble)
                  <*> (comma *> maybeDouble)
                  <*> (comma *> maybeDouble)
                  <*> (comma *> maybeDouble)
                  <*> (comma *> maybeDouble)

int :: Parser Int
int = fmap fromInteger decimal
date :: Parser Day
date = fromGregorian <$> decimal <*> (dash *> int) <*> (dash *> int)

eol :: Parser Text
eol = try (string "\n\r") <|> try (string "\r\n") <|> string "\n" <|> string "\r" <?> "end of line"

header1 :: Parser Day
header1 = date <* (comma *> string "全銘柄日足") <* (comma *> string "http://k-db.com/")

header2 :: Parser ()
header2 = string "コード,銘柄名,市場,業種,始値,高値,安値,終値,出来高,売買代金" *> return ()

stocks :: Parser [Stock]
stocks = header1 <* eol <* header2 <* eol >>= \d -> many' (stock d <* eol)
  
fromLazy :: TL.Text -> Text
fromLazy = T.pack . TL.unpack

sjis2utf8 :: BL.ByteString -> IO BL.ByteString
sjis2utf8 s = do
  (inp, outp, _, _) <- runInteractiveProcess "iconv" ["-f","SHIFT_JIS","-t","UTF-8"] Nothing Nothing
  BL.hPutStr inp s
  hClose inp
  BL.hGetContents outp

mkURL :: String -> String
mkURL d = "http://k-db.com/?p=all&download=csv&date=" ++ d

main :: IO ()
main = do
  (ymd:_) <- getArgs
  rsp <- simpleHTTP $ getRequest $ mkURL ymd
  str <- getResponseBody rsp
  let sjis = BL.pack str
  utf8 <- sjis2utf8 sjis
  let utf8text = fromLazy $ decodeUtf8 utf8
  con <- mkCon
  either putStr (mapM_ $ insert con) $ parseOnly stocks utf8text

check :: IO ()
check = check' 0 10

check' :: Int -> Int -> IO ()
check' f t = do
  (ymd:_) <- getArgs
  rsp <- simpleHTTP $ getRequest $ mkURL ymd
  str <- getResponseBody rsp
  let sjis = BL.pack str
  utf8 <- sjis2utf8 sjis
  let utf8text = fromLazy $ decodeUtf8 utf8
  either putStr (mapM_ printStock . take t . drop f) $ parseOnly stocks utf8text

printStock :: Stock -> IO ()
printStock x = do
  T.putStr $ stockCode x `T.snoc` ','
  T.putStr $ stockName x `T.snoc` ','
  T.putStr $ stockMarket x `T.snoc` ','
  T.putStrLn $ stockCategory x

stock'sBrand :: Stock -> Brand
stock'sBrand = Brand <$> stockCode <*> stockName <*> stockMarket <*> stockCategory <*> stockDay

brandKey :: Brand -> BrandKey
brandKey = BrandKey <$> brandCode <*> brandMarket

instance ToRow BrandKey where
  toRow d = [ toField (brandKeyCode d)
            , toField (brandKeyMarket d)
            ]

instance FromRow Brand where
  fromRow = Brand <$> field <*> field <*> field <*> field <*> field
instance ToRow Brand where
  toRow d = [ toField (brandName d)
            , toField (brandCategory d)
            , toField (brandLastUpdated d)
            , toField (brandCode d)
            , toField (brandMarket d)
            ]

instance FromRow Stock where
  fromRow = Stock <$> field <*> field <*> field <*> field <*> field <*> field <*> field <*> field <*> field <*> field <*> field
instance ToRow Stock where
  toRow d = [ toField (stockDay d)
            , toField (stockCode d)
            , toField (stockName d)
            , toField (stockMarket d)
            , toField (stockCategory d)
            , toField (stockOpeningPrice d)
            , toField (stockHighPrice d)
            , toField (stockLowPrice d)
            , toField (stockClosingPrice d)
            , toField (stockVolumeOfTrading d)
            , toField (stockTradingValue d)
            ]


mkCon :: IO Connection
mkCon = connect defaultConnectInfo { connectUser = "cutsea110", connectPassword = "cutsea110", connectDatabase = "kabu" }

insert :: Connection -> Stock -> IO Int64
insert con s = do
  upsert con (stock'sBrand s)
  insertStock con s

insertStock :: Connection -> Stock -> IO Int64
insertStock con =
  execute con "insert into stock (day,code,name,market,category,openingprice,highprice,lowprice,closingprice,volumeoftrading,tradingvalue) values (?,?,?,?,?,?,?,?,?,?,?)"

insertBrand :: Connection -> Brand -> IO Int64
insertBrand con =
  execute con "insert into brand (code,name,market,category,lastupdated) values (?,?,?,?,?)"

updateBrand :: Connection -> Brand -> IO Int64
updateBrand con =
  execute con "update brand set name = ? , category = ? , lastupdated = ? where code = ? and market = ?"

type Code = Text
type Name = Text

collect :: Connection -> Either Code Name -> IO [Stock]
collect con (Left cd) =
  query con "select * from stock where code = ?" (Only cd)
collect con (Right nm) =
  query con "select * from stock where name = ?" (Only nm)

get :: Connection -> Either Code Name -> IO (Maybe Stock)
get con (Left cd) =
  fmap (find (const True)) $ query con "select * from stock where code = ? limit 1" (Only cd)
get con (Right nm) =
  fmap (find (const True)) $ query con "select * from stock where name = ? limit 1" (Only nm)

upsert :: Connection -> Brand -> IO Int64
upsert con b = maybe (insertBrand con b) (const $ updateBrand con b) =<< exists con (brandKey b)

exists :: Connection -> BrandKey -> IO (Maybe Brand)
exists con b =
  fmap (find (const True)) $ query con "select * from brand where code = ? and market = ?" b
