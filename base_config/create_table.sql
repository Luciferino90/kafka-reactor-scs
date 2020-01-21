CREATE TABLE `kafka` (
                         `id` varchar(255) CHARACTER SET latin1 NOT NULL,
                         `ackedby` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
                         `msgid` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
                         `msgtype` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
                         `producerid` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
                         `ackreceived` int(11) DEFAULT NULL,
                         `partitionnumber` int(11) DEFAULT NULL,
                         `offsetnumber` int(11) DEFAULT NULL,
                         PRIMARY KEY (`id`),
                         KEY `msgid_idx` (`msgid`),
                         KEY `msgtype_idx` (`msgtype`),
                         KEY `producerid_idx` (`producerid`),
                         KEY `offset_idx` (`offsetnumber`),
                         KEY `partition_idx` (`partitionnumber`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
