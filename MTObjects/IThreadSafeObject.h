#pragma once

#include <unordered_set>
#include <vector>
#include <algorithm>
#include <assert.h>

namespace MTObjects
{
using std::unordered_set;
using std::vector;

struct IThreadSafeObject
{
private:
	friend struct Cluster;
	Cluster* cluster_ = nullptr;

public:
	virtual void IsDependentOn(vector<IThreadSafeObject*>& ref_dependencies) const = 0;
	virtual void IsConstDependentOn(vector<const IThreadSafeObject*>& ref_dependencies) const = 0;
	virtual int GetIndex() const = 0;
};

struct Cluster
{
	vector<IThreadSafeObject*> objects_;			  //with duplicates
	vector<const IThreadSafeObject*> const_dependencies_; //with duplicates
	unordered_set<const Cluster*> const_dependencies_clusters_;
	//unordered_set<IThreadSafeObject*> unique_objects_;
	int index = -1;

	void Reset()
	{
		objects_.clear();
		objects_.reserve(16);
		const_dependencies_.clear();
		const_dependencies_.reserve(16);
		const_dependencies_clusters_.clear();
		const_dependencies_clusters_.reserve(16);

		index = -1;
	}

	Cluster()
	{
		Reset();
	}

private:

	void MergeTo(Cluster& main_cluster)
	{
		for (auto obj : objects_)
		{
			obj->cluster_ = &main_cluster;
		}
		main_cluster.objects_.insert(main_cluster.objects_.end(), objects_.begin(), objects_.end());
		main_cluster.const_dependencies_.insert(main_cluster.const_dependencies_.end(), const_dependencies_.begin(), const_dependencies_.end());

		//At this stage const_dependencies_clusters_ are not generated yet
	}

	static void ClearClustersInObjects(const vector<IThreadSafeObject*>& all_objects)
	{
		for (auto obj : all_objects)
		{
			obj->cluster_ = nullptr;
		}
	}

	static void RemoveCluster(vector<Cluster*> &clusters, Cluster* cluster)
	{
		const int last_index = clusters.size() - 1;
		const int index_to_reuse = cluster->index;
		if (last_index != index_to_reuse)
		{
			clusters[index_to_reuse] = clusters[last_index];
			clusters[index_to_reuse]->index = index_to_reuse;
		}
		clusters.pop_back();
	}

	void GatherObjects(IThreadSafeObject* in_obj, vector<IThreadSafeObject*>& remaining_objects_table, int& object_counter, unordered_set<Cluster*>& clusters_to_merge, vector<IThreadSafeObject*>& objects_to_handle)
	{
		objects_to_handle.clear();
		objects_to_handle.push_back(in_obj);

		while (!objects_to_handle.empty())
		{
			IThreadSafeObject* obj = objects_to_handle.back();
			objects_to_handle.pop_back();

			assert(obj);
			const int obj_index = obj->GetIndex();
			const bool not_yet_in_cluster = nullptr != remaining_objects_table[obj_index];
			assert(not_yet_in_cluster || obj->cluster_);

			if (not_yet_in_cluster)
			{
				assert(nullptr == obj->cluster_);

				objects_.emplace_back(obj);
				obj->cluster_ = this;
				obj->IsConstDependentOn(const_dependencies_);
				obj->IsDependentOn(objects_to_handle);

				remaining_objects_table[obj_index] = nullptr;
				object_counter--;
			}
			else if (obj->cluster_ != this)
			{
				clusters_to_merge.emplace(obj->cluster_);
			}
		}
	}

	static void CreateClusters(vector<IThreadSafeObject *> &all_objects, vector<Cluster*> &clusters, vector<Cluster>& preallocated_clusters)
	{
		unordered_set<Cluster*> clusters_to_merge;
		clusters_to_merge.reserve(128);
		vector<IThreadSafeObject*> objects_buff;
		objects_buff.reserve(128);
		int objects_counter = all_objects.size();
		unsigned int cluster_counter = 0;
		int first_remaining_obj_index = -1;
		while (objects_counter > 0)
		{
			assert(cluster_counter < preallocated_clusters.size());
			Cluster* cluster = &preallocated_clusters[cluster_counter];
			do { first_remaining_obj_index++; } while (nullptr == all_objects[first_remaining_obj_index]);
			cluster->GatherObjects(all_objects[first_remaining_obj_index], all_objects, objects_counter, clusters_to_merge, objects_buff);
			if (clusters_to_merge.empty())
			{
				cluster->index = clusters.size();
				clusters.emplace_back(cluster);
				cluster_counter++;
			}
			else
			{
				Cluster* main_cluster = *clusters_to_merge.begin();
				clusters_to_merge.erase(clusters_to_merge.begin());
				cluster->MergeTo(*main_cluster);
				cluster->Reset();
				for (auto other_cluster : clusters_to_merge)
				{
					other_cluster->MergeTo(*main_cluster);
					RemoveCluster(clusters, other_cluster);
				}
				clusters_to_merge.clear();
			}
		}
	}

	static void CreateClustersDependenciesAndRemoveDuplicates(const vector<Cluster*>& clusters)
	{
		for (auto cluster : clusters)
		{
			//Generate cluster dependencies. It can e done, only when all objects have cluster set.
			for (const IThreadSafeObject* obj : cluster->const_dependencies_)
			{
				if (obj->cluster_ != cluster)
				{
					cluster->const_dependencies_clusters_.emplace(obj->cluster_);
				}
			}
			//cluster->const_dependencies_.clear();
			//cluster->const_dependencies_.shrink_to_fit();

			//Remove duplicates:
			//cluster->unique_objects_.insert(cluster->objects_.begin(), cluster->objects_.end());
			//cluster->objects_.clear();
			//cluster->objects_.shrink_to_fit();
		}
	}

public:

	static vector<Cluster*> GenerateClusters(vector<IThreadSafeObject*>& all_objects, vector<Cluster>& preallocated_clusters)
	{
		ClearClustersInObjects(all_objects);

		vector<Cluster*> clusters;
		clusters.reserve(preallocated_clusters.size());
		CreateClusters(all_objects, clusters, preallocated_clusters);
		CreateClustersDependenciesAndRemoveDuplicates(clusters);
		return clusters;
	}
};

struct GroupOfConcurrentClusters : public vector<Cluster*>
{
	unordered_set<const Cluster*> const_dependencies_clusters_;

	void AddCluster(Cluster& cluster)
	{
		emplace_back(&cluster);
		const_dependencies_clusters_.insert(cluster.const_dependencies_clusters_.begin(), cluster.const_dependencies_clusters_.end());
	}

	static vector<GroupOfConcurrentClusters> GenerateClusterGroups(const vector<Cluster*>& clusters)
	{
		auto depends_on = [](const Cluster* a, const Cluster* b) -> bool
		{
			return a->const_dependencies_clusters_.end() != a->const_dependencies_clusters_.find(b);
		};

		auto fits_into_group = [&](const GroupOfConcurrentClusters& group, const Cluster* cluster) -> bool
		{
			return std::none_of(group.begin(), group.end(), [&](const Cluster* cluster_in_group)
			{
				return depends_on(cluster, cluster_in_group);
			}) 

			&& group.const_dependencies_clusters_.find(cluster) == group.const_dependencies_clusters_.end();
		};

		vector<GroupOfConcurrentClusters> groups;
		groups.reserve(clusters.size());
		groups.resize(std::max<int>(1, clusters.size() / 32));
		int cluster_index = 0;
		for (auto cluster : clusters)
		{
			bool fits_in_existing_group = false;
			{
				const int group_num = groups.size();
				const int first_group_to_try = cluster_index % group_num;
				for (int groups_checked = 0; groups_checked < group_num && !fits_in_existing_group; groups_checked++)
				{
					const int group_idx = (first_group_to_try + groups_checked) % group_num;
					auto& group = groups[group_idx];
					if (fits_into_group(group, cluster))
					{
						group.AddCluster(*cluster);
						fits_in_existing_group = true;
					}
				}
			}
			if (!fits_in_existing_group)
			{
				GroupOfConcurrentClusters new_group;
				new_group.AddCluster(*cluster);
				groups.emplace_back(new_group);
			}
			cluster_index++;
		}
		return groups;
	}
};
}