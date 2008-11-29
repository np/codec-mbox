{-# LANGUAGE BangPatterns, FlexibleInstances, TypeOperators #-}
--------------------------------------------------------------------
-- |
-- Module    : Codec.Mbox
-- Copyright : (c) Nicolas Pouillard 2008
-- License   : BSD3
--
-- Maintainer: Nicolas Pouillard <nicolas.pouillard@gmail.com>
-- Stability : provisional
-- Portability:
--
--------------------------------------------------------------------

module Codec.Mbox
  ( Mbox(..)
  , MboxMessage(..)
  , Direction(..)
  , Month(..)
  , mboxMsgSenderA
  , mboxMsgTimeA
  , mboxMsgBodyA
  , mboxMsgFileA
  , mboxMsgOffsetA
  , parseMboxFile
  , parseMboxFiles
  , parseMbox
  , safeParseMbox
  , parseOneMboxMessage
  , printMbox
  , printMboxMessage
  , printMboxFromLine
  , fromQuoting
  , msgYear
  , msgMonthYear
  ) where

import Control.Arrow (first,second)
import Control.Applicative ((<$>))
import qualified Data.ByteString.Lazy.Char8 as C -- Char8 interface over Lazy ByteString's
import Data.Accessor
import Data.ByteString.Lazy (ByteString)
import Data.Int (Int64)
import Data.Maybe (listToMaybe)
import System.IO
import System.IO.Unsafe (unsafeInterleaveIO)

data a :*: b = !a :*: !b

first' :: (a -> b) -> (a :*: c) -> (b :*: c)
first' f !(a :*: c) = (f a :*: c)
{-# INLINE first' #-}
uncurry' :: (a -> b -> c) -> a :*: b -> c
uncurry' f (x :*: y) = f x y
{-# INLINE uncurry' #-}

--import Test.QuickCheck

-- | An Mbox is a list of MboxMessage
newtype Mbox s = Mbox { unMbox :: [MboxMessage s] }
  deriving (Eq, Ord, Show)

-- | An MboxMessage represent an mbox message, featuring
-- the sender, the date-time, and the message body.
data MboxMessage s = MboxMessage { mboxMsgSender :: s
                                 , mboxMsgTime   :: s
                                 , mboxMsgBody   :: s
                                 , mboxMsgFile   :: FilePath
                                 , mboxMsgOffset :: Int64 }
  deriving (Eq, Ord, Show)

mboxMsgSenderA :: Accessor (MboxMessage s) s
mboxMsgSenderA = accessor mboxMsgSender (\x r -> r{mboxMsgSender=x})

mboxMsgTimeA :: Accessor (MboxMessage s) s
mboxMsgTimeA = accessor mboxMsgTime (\x r -> r{mboxMsgTime=x})

mboxMsgBodyA :: Accessor (MboxMessage s) s
mboxMsgBodyA = accessor mboxMsgBody (\x r -> r{mboxMsgBody=x})

mboxMsgFileA :: Accessor (MboxMessage s) FilePath
mboxMsgFileA = accessor mboxMsgFile (\x r -> r{mboxMsgFile=x})

mboxMsgOffsetA :: Accessor (MboxMessage s) Int64
mboxMsgOffsetA = accessor mboxMsgOffset (\x r -> r{mboxMsgOffset=x})

readYear :: MboxMessage C.ByteString -> C.ByteString -> Int
readYear m s =
 case reads $ C.unpack s of
   [(i, "")] -> i
   _         -> error ("readYear: badly formatted date (year) in " ++ show (C.unpack $ mboxMsgTime m))

data Month = Jan
           | Feb
           | Mar
           | Apr
           | May
           | Jun
           | Jul
           | Aug
           | Sep
           | Oct
           | Nov
           | Dec
  deriving (Read, Show, Eq)

readMonth :: MboxMessage C.ByteString -> C.ByteString -> Month
readMonth m s =
  case reads $ C.unpack s of
    [(month, "")] -> month
    _             -> error ("readMonth: badly formatted date (month) in " ++ show (C.unpack $ mboxMsgTime m))

msgYear :: MboxMessage C.ByteString -> Int
msgYear m = readYear m . last . C.split ' ' . mboxMsgTime $ m

msgMonthYear :: MboxMessage C.ByteString -> (Month,Int)
msgMonthYear m =
 case filter (not . C.null) . C.split ' ' . mboxMsgTime $ m of
   [_wday, month, _mday, _hour, year] -> (readMonth m month, readYear m year)
   _ -> error ("msgMonthYear: badly formatted date in " ++ show (C.unpack $ mboxMsgTime m))

nextFrom :: ByteString -> Maybe (Int64, ByteString, ByteString)
nextFrom !orig = goNextFrom 0 orig
   where goNextFrom !count !input = do
           off <- (+1) <$> C.elemIndex '\n' input
           let (nls, i') = first C.length $ C.span (=='\n') $ C.drop off input
           if C.take 5 i' == bFrom
            then let off' = off + count + (nls - 1) in
                 Just (off', C.take off' orig, C.drop 5 i')
            else goNextFrom (off + count + nls) i'

-- TODO rules:
--  - fromQuoting id == id
--  - f . g == id ==> fromQuoting f . fromQuoting g == id
--    but fromQuoting can fail on negative levels
--  special case that works:
--     n > 0 ==> fromQuoting ((-)n) . fromQuoting (+n) == id

-- TODO performances:
--   This ByteString fromQuoting is already quite fast,
--   It still implies a x2 factor to the mbox-average-size tool, wether one
--   enables it or not.
--   Perhaps fusing nextFrom and fromQuoting could give more performances
--   (but will decrease performances of mbox-counting).

-- | Quoted from http://qmail.org./man/man5/mbox.html:
--
-- @
--   \>From quoting ensures that the resulting
--   lines are not From_ lines:  the program prepends a \> to any
--   From_ line, \>\From_ line, \>\>From_ line, \>\>\>From_ line, etc.
-- @
fromQuoting :: (Int64 -> Int64) -> C.ByteString -> C.ByteString
fromQuoting onLevel = C.tail . nextQuotedFrom . C.cons '\n'
  where nextQuotedFrom !orig = goNextQuotedFrom 0 orig
         where goNextQuotedFrom !count !input =
                case C.elemIndex '\n' input of
                 Nothing -> orig
                 Just off ->
                   let (!level, i') = first C.length $ C.span (=='>') $ C.drop (off + 1) input in
                   if C.take 5 i' == bFrom
                    then (C.take (off + count) orig) `C.append`
                         mkQuotedFrom (onLevel level) `C.append`
                         nextQuotedFrom (C.drop 5 i')
                    else goNextQuotedFrom (off + level + count + 1) i'

{-
prop_fromQuotingInv (NonNegative n) s = s == fromQuoting (+(-n)) (fromQuoting (+n) s)
prop_unparse_parse m = either (const False) (==m) $ safeParseMbox (printMbox m)
prop_parse_unparse m = let s = printMbox m in Right s == (printMbox <$> safeParseMbox s)

-- | Prefered xs: have the xs elements as favorites.
newtype Prefered a = Prefered { unPrefered :: a }
 deriving ( Eq, Ord, Show, Read )

class Favorites a where
  favorites :: [a]

instance Favorites Char where
  favorites = ">From \n"

instance Arbitrary C.ByteString where
    arbitrary = arbitrary >>= return . C.pack . map unPrefered

instance (Favorites a, Arbitrary a) => Arbitrary (Prefered a) where
  arbitrary =
    frequency
      [ (2, Prefered `fmap` oneof (map return favorites))
      , (1, Prefered `fmap` arbitrary)
      ]

  shrink (Prefered x) = Prefered `fmap` shrink x

--instance Arbitrary s => Arbitrary (MboxMessage s) where
instance Arbitrary (MboxMessage ByteString) where
  arbitrary = MboxMessage (C.pack "S") (C.pack "D") <$> arbitrary -- TODO better sender,time
  shrink (MboxMessage x y z) = [ MboxMessage x' y' z' | (x', y', z') <- shrink (x, y, z) ]

instance Arbitrary (Mbox ByteString) where
--instance Arbitrary s => Arbitrary (Mbox s) where
  arbitrary = Mbox <$> arbitrary
  shrink (Mbox x) = Mbox <$> shrink x
-}

mkQuotedFrom :: Int64 -> C.ByteString
mkQuotedFrom n | n < 0     = error "mkQuotedFrom: negative quoting"
               | otherwise = '\n' `C.cons` C.replicate n '>' `C.append` bFrom


bFrom :: ByteString
bFrom = C.pack "From "

skipFirstFrom :: ByteString -> Either String ByteString
skipFirstFrom xs | bFrom == C.take 5 xs = Right $ C.drop 5 xs
                 | otherwise = Left "skipFirstFrom: badly formatted mbox: 'From ' expected at the beginning"

-- | Same as parseMbox but cat returns an error message.
safeParseMbox :: FilePath -> Int64 -> ByteString -> Either String (Mbox ByteString)
safeParseMbox fp offset s | C.null s  = Right $ Mbox []
                          | otherwise = (Mbox . map (uncurry' $ finishMboxMessageParsing fp) . splitMboxMessages offset)
                                         <$> (skipFirstFrom s)

-- | Turns a ByteString into an Mbox by splitting on 'From' lines and
-- unquoting the '>*From's of the message.
parseMbox :: ByteString -> Mbox ByteString
parseMbox = either error id . safeParseMbox "" 0

splitMboxMessages :: Int64 -> ByteString -> [Int64 :*: ByteString]
splitMboxMessages !offset !input =
  case nextFrom input of
    Nothing | C.null input      -> []
            | otherwise         -> [(offset :*: input)]
    Just (!offset', !msg, rest) -> (offset :*: msg) : splitMboxMessages (6 + offset + offset') rest

finishMboxMessageParsing :: FilePath -> Int64 -> ByteString -> MboxMessage ByteString
finishMboxMessageParsing fp !offset !inp = MboxMessage sender time (fromQuoting pred body) fp offset
  where ((sender,time),body) = first (breakAt ' ') $ breakAt '\n' inp
        breakAt c = second (C.drop 1 {- a safe tail -}) . C.break (==c)

-- | Turns an mbox into a ByteString
printMbox :: Mbox ByteString -> ByteString
printMbox = C.intercalate (C.singleton '\n') . map printMboxMessage . unMbox

-- | Returns an header line in mbox format given an mbox message.
printMboxFromLine :: MboxMessage ByteString -> ByteString
printMboxFromLine (MboxMessage sender time _ _ _) =
  C.append bFrom
    $ C.append sender
    $ C.cons   ' '
    $ C.append time
    $ C.cons   '\n'
    $ C.empty

-- | Returns a ByteString given an mbox message.
printMboxMessage :: MboxMessage ByteString -> ByteString
printMboxMessage msg = printMboxFromLine msg `C.append` fromQuoting (+1) (mboxMsgBody msg)

-- lazyness at work!

-- | Given a file handle and an offset, parseOneMboxMessage returns
-- the message a this offset.
parseOneMboxMessage :: FilePath -> Handle -> Integer -> IO (MboxMessage C.ByteString)
parseOneMboxMessage fp fh offset = do
  hSeek fh AbsoluteSeek offset
  s <- C.hGetContents fh
  a <- either fail return $ safeParseMbox fp (fromInteger offset) s
  (maybe (fail "parseOneMboxMessage: end of file") return . listToMaybe . unMbox) a

readRevMboxFile :: FilePath -> IO (Mbox ByteString)
readRevMboxFile fp = readRevMboxHandle fp =<< openFile fp ReadMode

-- | @readRevMboxHandle fp h@ returns a reversed mbox for a file handle.
-- The file handle is supposed to be in text mode, readable.
-- buffering?
readRevMboxHandle :: FilePath -> Handle -> IO (Mbox ByteString)
readRevMboxHandle fp fh = do siz <- hFileSize fh
                             readRevMbox fp siz <$> readHandleBackward mboxChunkSize siz fh

readRevMbox :: FilePath -> Integer -> [ByteString] -> Mbox ByteString
readRevMbox fp filesize chunks = Mbox $ go (fromInteger filesize+1) (filter (not . C.null) chunks)
  where go  _   []          = []
        go !siz (chunk1:cs) =
                  case nextFrom chunk1 of
                    Nothing                         -> kont siz cs chunk1
                    Just (!_backoffset, !msg, rest) ->
                      let siz' = siz - C.length rest - 6 in
                      (map finishmmp . reverse . map (first' (+siz')) . splitMboxMessages 0 $ rest)
                      ++ kont siz' cs msg

        kont !_   []           = (:[]) . finishLast
        kont !siz (chunk2:cs2) = \k -> go siz (chunk2 `C.append` k : cs2)

        finishLast =
          finishMboxMessageParsing fp 0 . either (error . ("readRevMbox: impossible: " ++)) id . skipFirstFrom

        finishmmp = uncurry' $ finishMboxMessageParsing fp

-- | @readHandleBackward maxChunkSize size h@ lazily reads the @h@ file handle
-- from the end. The file contents is returned as a reversed list of chunks.
-- The result is such that if one apply @C.concat . reverse@ one get
-- the in-order contents.
{-
propIO_read_anydir (Positive maxChunkSize) fh =
   do xs <- hGetContent fh
      siz <- hFileSize fh
      ys <- readHandleBackward maxChunkSize siz fh
      xs == C.concat (reverse ys)
-}
readHandleBackward :: Integer -> Integer -> Handle -> IO [ByteString]
readHandleBackward maxChunkSize siz0 fh = go siz0
  where go 0   = return []
        go siz = unsafeInterleaveIO $
          do let delta = min maxChunkSize siz
                 siz'  = siz - delta
             hSeek fh AbsoluteSeek siz'
             s <- C.hGet fh $ fromInteger delta
             (s :) <$> go siz'

data Direction = Backward | Forward

-- | Returns a mbox given a direction (forward/backward) and a file path.
parseMboxFile :: Direction -> FilePath -> IO (Mbox ByteString)
parseMboxFile Forward  fp = (either fail return =<<) . (safeParseMbox fp 0 <$>) . C.readFile $ fp
parseMboxFile Backward fp = readRevMboxFile fp

-- | Returns a mbox list given a direction (forward/backward) and a list of file path.
--   Note that files are opened lazily.
parseMboxFiles :: Direction -> [FilePath] -> IO [Mbox ByteString]
parseMboxFiles Forward  [] = (:[]) . parseMbox <$> C.getContents
parseMboxFiles Backward [] = fail "reading backward on standard input does not make sense"
parseMboxFiles dir      xs = mapM (unsafeInterleaveIO . parseMboxFile dir) xs

mboxChunkSize :: Integer
mboxChunkSize = 10*oneMegabyte

-- one megabyte in bytes
oneMegabyte :: Integer
oneMegabyte = 2 ^ (20 :: Int)
