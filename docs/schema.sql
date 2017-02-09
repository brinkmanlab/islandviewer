-- phpMyAdmin SQL Dump
-- version 3.4.11.1deb1
-- http://www.phpmyadmin.net
--
-- Host: mysql2
-- Generation Time: Aug 20, 2015 at 02:18 PM
-- Server version: 5.5.28
-- PHP Version: 5.4.38-0+deb7u1

SET SQL_MODE="NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";

--
-- Database: `islandviewer`
--

-- --------------------------------------------------------

--
-- Table structure for table `Analysis`
--

CREATE TABLE IF NOT EXISTS `Analysis` (
  `aid` int(11) NOT NULL AUTO_INCREMENT,
  `atype` int(11) NOT NULL,
  `ext_id` varchar(24) NOT NULL,
  `owner_id` int(11) NOT NULL,
  `token` varchar(22) DEFAULT NULL,
  `default_analysis` tinyint(1) NOT NULL,
  `status` int(11) NOT NULL,
  `workdir` varchar(100) NOT NULL,
  `microbedb_ver` int(11) NOT NULL,
  `start_date` datetime NOT NULL,
  `complete_date` datetime NOT NULL,
  PRIMARY KEY (`aid`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1 AUTO_INCREMENT=7082 ;

--
-- Triggers `Analysis`
--
DROP TRIGGER IF EXISTS `updatestatus`;
DELIMITER //
CREATE TRIGGER `updatestatus` BEFORE UPDATE ON `Analysis`
 FOR EACH ROW BEGIN
    IF NEW.status = 2 THEN
      SET NEW.start_date = NOW();
    ELSEIF NEW.status = 3 OR NEW.status = 4 THEN
      SET NEW.complete_date = NOW();
    END IF;
    END
//
DELIMITER ;

-- --------------------------------------------------------

--
-- Table structure for table `auth_group`
--

CREATE TABLE IF NOT EXISTS `auth_group` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(80) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `auth_group_permissions`
--

CREATE TABLE IF NOT EXISTS `auth_group_permissions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` int(11) NOT NULL,
  `permission_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `group_id` (`group_id`,`permission_id`),
  KEY `auth_group_permissions_5f412f9a` (`group_id`),
  KEY `auth_group_permissions_83d7f98b` (`permission_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `auth_permission`
--

CREATE TABLE IF NOT EXISTS `auth_permission` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(50) NOT NULL,
  `content_type_id` int(11) NOT NULL,
  `codename` varchar(100) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `content_type_id` (`content_type_id`,`codename`),
  KEY `auth_permission_37ef4eb4` (`content_type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `auth_user`
--

CREATE TABLE IF NOT EXISTS `auth_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `password` varchar(128) NOT NULL,
  `last_login` datetime NOT NULL,
  `is_superuser` tinyint(1) NOT NULL,
  `username` varchar(30) NOT NULL,
  `first_name` varchar(30) NOT NULL,
  `last_name` varchar(30) NOT NULL,
  `email` varchar(75) NOT NULL,
  `is_staff` tinyint(1) NOT NULL,
  `is_active` tinyint(1) NOT NULL,
  `date_joined` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `auth_user_groups`
--

CREATE TABLE IF NOT EXISTS `auth_user_groups` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `group_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id` (`user_id`,`group_id`),
  KEY `auth_user_groups_6340c63c` (`user_id`),
  KEY `auth_user_groups_5f412f9a` (`group_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `auth_user_user_permissions`
--

CREATE TABLE IF NOT EXISTS `auth_user_user_permissions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `permission_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id` (`user_id`,`permission_id`),
  KEY `auth_user_user_permissions_6340c63c` (`user_id`),
  KEY `auth_user_user_permissions_83d7f98b` (`permission_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `CustomGenome`
--

CREATE TABLE IF NOT EXISTS `CustomGenome` (
  `cid` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(60) NOT NULL,
  `owner_id` int(11) NOT NULL DEFAULT '0',
  `cds_num` int(11) NOT NULL,
  `rep_size` int(11) NOT NULL,
  `filename` varchar(100) DEFAULT NULL,
  `formats` varchar(50) NOT NULL,
  `contigs` int(11) NOT NULL DEFAULT '1',
  `genome_status` enum('NEW','UNCONFIRMED','MISSINGSEQ','MISSINGCDS','VALID','READY','INVALID') NOT NULL DEFAULT 'NEW',
  `submit_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`cid`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1 AUTO_INCREMENT=3158 ;

-- --------------------------------------------------------

--
-- Table structure for table `Distance`
--

CREATE TABLE IF NOT EXISTS `Distance` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `rep_accnum1` varchar(24) NOT NULL,
  `rep_accnum2` varchar(24) NOT NULL,
  `distance` double NOT NULL,
  PRIMARY KEY (`id`),
  KEY `accnum1_index` (`rep_accnum1`,`rep_accnum2`),
  KEY `accnum2_index` (`rep_accnum2`,`rep_accnum1`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1 AUTO_INCREMENT=15868192 ;

-- --------------------------------------------------------

--
-- Table structure for table `django_content_type`
--

CREATE TABLE IF NOT EXISTS `django_content_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `app_label` varchar(100) NOT NULL,
  `model` varchar(100) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `app_label` (`app_label`,`model`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `django_session`
--

CREATE TABLE IF NOT EXISTS `django_session` (
  `session_key` varchar(40) NOT NULL,
  `session_data` longtext NOT NULL,
  `expire_date` datetime NOT NULL,
  PRIMARY KEY (`session_key`),
  KEY `django_session_b7b81f0c` (`expire_date`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `django_site`
--

CREATE TABLE IF NOT EXISTS `django_site` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `domain` varchar(100) NOT NULL,
  `name` varchar(50) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `GC`
--

CREATE TABLE IF NOT EXISTS `GC` (
  `ext_id` varchar(24) NOT NULL,
  `min` double NOT NULL,
  `max` double NOT NULL,
  `mean` double NOT NULL,
  `gc` longtext NOT NULL,
  PRIMARY KEY (`ext_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `Genes`
--

CREATE TABLE IF NOT EXISTS `Genes` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `ext_id` varchar(24) NOT NULL,
  `start` int(11) NOT NULL,
  `end` int(11) NOT NULL,
  `strand` tinyint(4) NOT NULL,
  `name` varchar(18) DEFAULT NULL,
  `gene` varchar(10) DEFAULT NULL,
  `product` varchar(100) DEFAULT NULL,
  `locus` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `dup_catcher` (`ext_id`,`start`,`end`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1 AUTO_INCREMENT=20075852 ;

-- --------------------------------------------------------

--
-- Table structure for table `GenomicIsland`
--

CREATE TABLE IF NOT EXISTS `GenomicIsland` (
  `gi` int(11) NOT NULL AUTO_INCREMENT,
  `aid_id` int(11) NOT NULL,
  `start` int(11) NOT NULL,
  `end` int(11) NOT NULL,
  `prediction_method` varchar(15) NOT NULL,
  `details` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`gi`),
  KEY `GenomicIsland_1fe21307` (`aid_id`,`prediction_method`),
  KEY `prediction_method` (`prediction_method`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1 AUTO_INCREMENT=232602 ;

-- --------------------------------------------------------

--
-- Table structure for table `GIAnalysisTask`
--

CREATE TABLE IF NOT EXISTS `GIAnalysisTask` (
  `taskid` int(11) NOT NULL AUTO_INCREMENT,
  `aid_id` int(11) NOT NULL,
  `prediction_method` varchar(15) NOT NULL,
  `status` int(11) NOT NULL,
  `parameters` text,
  `start_date` datetime NOT NULL,
  `complete_date` datetime NOT NULL,
  PRIMARY KEY (`taskid`),
  KEY `GIAnalysisTask_1fe21307` (`aid_id`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1 AUTO_INCREMENT=49583 ;

-- --------------------------------------------------------

--
-- Table structure for table `IslandGenes`
--

CREATE TABLE IF NOT EXISTS `IslandGenes` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `gi` int(11) NOT NULL,
  `gene_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `gene_id_refs_id_eac26c32` (`gene_id`),
  KEY `gi_index` (`gi`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1 AUTO_INCREMENT=5891632 ;

-- --------------------------------------------------------

--
-- Table structure for table `IslandPick`
--

CREATE TABLE IF NOT EXISTS `IslandPick` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `taskid_id` int(11) NOT NULL,
  `reference_rep_accnum` varchar(100) NOT NULL,
  `alignment_program` varchar(50) NOT NULL,
  `min_gi_size` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `IslandPick_752fe31f` (`taskid_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `NameCache`
--

CREATE TABLE IF NOT EXISTS `NameCache` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cid` varchar(24) NOT NULL,
  `name` varchar(100) NOT NULL,
  `cds_num` int(11) NOT NULL,
  `rep_size` int(11) NOT NULL,
  `isvalid` tinyint(4) NOT NULL DEFAULT '1',
  PRIMARY KEY (`id`),
  UNIQUE KEY `ext_id_index` (`cid`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1 AUTO_INCREMENT=4301 ;

-- --------------------------------------------------------

--
-- Table structure for table `Notification`
--

CREATE TABLE IF NOT EXISTS `Notification` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `analysis_id` int(11) NOT NULL,
  `email` varchar(75) NOT NULL,
  `status` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `analysis_id_refs_aid_4715085b` (`analysis_id`,`email`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1 AUTO_INCREMENT=4007 ;

-- --------------------------------------------------------

--
-- Table structure for table `SiteStatus`
--

CREATE TABLE IF NOT EXISTS `SiteStatus` (
  `status` int(11) NOT NULL DEFAULT '0',
  `message` text
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `UploadCounter`
--

CREATE TABLE IF NOT EXISTS `UploadCounter` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `ip_addr` char(15) NOT NULL,
  `date_uploaded` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- Table structure for table `UploadGenome`
--

CREATE TABLE IF NOT EXISTS `UploadGenome` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `filename` varchar(120) DEFAULT NULL,
  `ip_addr` char(15) DEFAULT NULL,
  `genome_name` varchar(40) DEFAULT NULL,
  `email` varchar(75) DEFAULT NULL,
  `cid` int(11) DEFAULT '0',
  `date_uploaded` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1 AUTO_INCREMENT=3158 ;

-- --------------------------------------------------------

--
-- Table structure for table `virulence`
--

CREATE TABLE IF NOT EXISTS `virulence` (
  `protein_accnum` varchar(18) NOT NULL DEFAULT '',
  `external_id` varchar(18) NOT NULL DEFAULT '',
  `source` enum('VFDB','ARDB','PAG','CARD','RGI','Victors','PATRIC_VF','BLAST',',') DEFAULT NULL,
  `type` enum('resistance','virulence','pathogen-associated') NOT NULL,
  `flag` text,
  `pmid` int(12) DEFAULT NULL,
  `date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`protein_accnum`,`external_id`),
  KEY `protein_accnum` (`protein_accnum`),
  KEY `external_id` (`external_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `virulence_curated_reps`
--

CREATE TABLE IF NOT EXISTS `virulence_curated_reps` (
  `rep_accnum` varchar(24) NOT NULL,
  PRIMARY KEY (`rep_accnum`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `virulence_mapped`
--

CREATE TABLE IF NOT EXISTS `virulence_mapped` (
  `gene_id` int(11) NOT NULL DEFAULT '0',
  `ext_id` varchar(24) NOT NULL,
  `protein_accnum` varchar(18) DEFAULT NULL,
  `external_id` varchar(18) NOT NULL DEFAULT '',
  `source` enum('VFDB','ARDB','PAG','CARD','RGI','Victors','PATRIC_VF','BLAST',',') DEFAULT NULL,
  `type` enum('resistance','virulence','pathogen-associated') NOT NULL,
  `flag` text,
  `pmid` varchar(50) DEFAULT NULL,
  `date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`external_id`,`gene_id`,`ext_id`),
  KEY `protein_accnum` (`protein_accnum`),
  KEY `external_id` (`external_id`),
  KEY `xref_join` (`ext_id`,`gene_id`),
  KEY `gene_id` (`gene_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
