package main

import (
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	awsBrooker "github.com/itslearninggermany/itswizard_m_awsbrooker"
	itswizard_basic "github.com/itslearninggermany/itswizard_m_basic"
	"github.com/jinzhu/gorm"
	"log"
)

const institutionID = "24"

func main() {
	// Datenbank einrichten
	var databaseConfig []itswizard_basic.DatabaseConfig
	b, _ := awsBrooker.DownloadFileFromBucket("brooker", "admin/databaseconfig.json")
	err := json.Unmarshal(b, &databaseConfig)
	if err != nil {
		fmt.Println("Error by reading database file", err)
		return
	}
	allDatabases := make(map[string]*gorm.DB)
	for i := 0; i < len(databaseConfig); i++ {
		database, err := gorm.Open(databaseConfig[i].Dialect, databaseConfig[i].Username+":"+databaseConfig[i].Password+"@tcp("+databaseConfig[i].Host+")/"+databaseConfig[i].NameOrCID+"?charset=utf8&parseTime=True&loc=Local")
		if err != nil {
			log.Println(err)
		}
		allDatabases[databaseConfig[i].NameOrCID] = database
	}

	// Checken ob ein Cache da ist
	var dumpFromDb itswizard_basic.UpdateUcsDump

	err = allDatabases[institutionID].Where("cache_to_run = 1").Last(&dumpFromDb).Error
	if err != nil {
		fmt.Println(err)
	}

	by, _ := awsBrooker.DownloadFileFromBucket("itswizard", "ucsdump/"+dumpFromDb.Cache)

	runCache(by, allDatabases[institutionID])

	dumpFromDb.CacheRunned = true

	err = allDatabases[institutionID].Save(&dumpFromDb).Error
	if err != nil {
		fmt.Println(err)
	}

}

func runCache(input []byte, db *gorm.DB) {
	var data []UcsDumpcacheUser

	err := json.Unmarshal(input, &data)
	if err != nil {
		fmt.Println(err)
	}

	for _, v := range data {
		var user itswizard_basic.UniventionPerson
		err = db.Where("username  = ?", v.Username).Last(&user).Error
		if err != nil {
			fmt.Println(err)
		}
		if v.UpdateFirstName {
			user.FirstName = v.NewFirstName
			user.ToUpdate = true
			user.Success = false
			user.UdpateFirstName = true
		}
		if v.UpdateLastName {
			user.LastName = v.NewLastName
			user.ToUpdate = true
			user.Success = false
			user.UdpateLastName = true
		}
		if v.UpdateProfile {
			user.Profile = v.NewProfile
			user.ToUpdate = true
			user.Success = false
			user.UdpateProfile = true
		}
		if v.UpdateStammschule {
			user.Stammschule = v.NewStammschule
			user.ToUpdate = true
			user.Success = false
			user.UpdateStammschule = true
		}
		if v.UpdateSchoolMembership {
			user.Schulmitgliedschaften = v.NewSchoolMemberships
			user.ToUpdate = true
			user.Success = false
			user.UpdateSchulmitgliedschaften = true
		}
		if v.UpdateGroupMembership {
			user.GruppenMitgliedschaften = v.NewGroupMembership
			user.ToUpdate = true
			user.Success = false
			user.UpdateGruppenMitgliedschaften = true
		}
		if v.DeleteUser {
			user.FirstName = v.NewFirstName
			user.ToUpdate = false
			user.ToDelete = true
			user.Success = false
			user.UdpateFirstName = true
		}
		err = db.Save(&user).Error
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(user.Username)

	}
}

type UcsDumpcacheUser struct {
	Number                 uint
	Username               string
	UpdateFirstName        bool
	NewFirstName           string
	UpdateLastName         bool
	NewLastName            string
	UpdateProfile          bool
	NewProfile             string
	UpdateStammschule      bool
	NewStammschule         string
	UpdateSchoolMembership bool
	NewSchoolMemberships   string
	UpdateGroupMembership  bool
	NewGroupMembership     string
	DeleteUser             bool
	ImportUser             bool
}
